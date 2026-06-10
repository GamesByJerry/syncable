import 'dart:async';

/// Per-circle encryption mode, resolved at push/pull time (one circle can be
/// encrypted while another stays plaintext in the same sync loop).
///
/// The staged rollout lifecycle is:
/// `off` → `shadow` (dual-write + parity verify) → `enforced` (ciphertext
/// only), and back down via decrypt-and-restore at any point before the
/// legacy plaintext columns are dropped.
enum SyncEncryptionMode {
  /// Plaintext pass-through — the default. Rows of circles in this mode sync
  /// exactly as without a cipher, except that tables *registered* for
  /// encryption push an explicit `content_enc = null` so a circle dropping
  /// back from `shadow`/`enforced` sheds its stale blobs (the
  /// decrypt-and-restore direction).
  off,

  /// Dual-write: pushes carry the plaintext columns AND the encrypted blob;
  /// on pull the blob is decrypted and verified against the plaintext fields
  /// (mismatches are reported via the alert hook). Plaintext stays
  /// authoritative — there is no privacy gain yet, this mode exists to prove
  /// the pipeline lossless before a circle is promoted.
  shadow,

  /// Ciphertext only: pushes null the registered plaintext columns and rows
  /// are read from the blob. Rows that cannot be decrypted (missing key,
  /// unknown key version, failed authentication) are stored *locked* — the
  /// ciphertext is preserved locally and decryption is re-attempted when new
  /// key material arrives.
  enforced,
}

/// The result of encrypting a row's content fields: the AEAD blob (wire
/// `content_enc` column) and the key version it was encrypted under (wire
/// `key_version` column).
class SyncEncryptedBlob {
  const SyncEncryptedBlob({required this.contentEnc, required this.keyVersion});

  final String contentEnc;
  final int keyVersion;
}

/// Thrown by a [SyncFieldCipher] when the key needed for an operation is not
/// available (no circle key for the circle, or an unknown `key_version`).
///
/// This is a key-*availability* problem, not a cryptographic failure: the sync
/// engine keeps the ciphertext (locked row / deferred push) and reports it at
/// [SyncEncryptionAlertSeverity.info]. Decryption is re-attempted from the
/// local fallback once new key material arrives ([SyncManager.retryLockedRows]).
class SyncCipherMissingKeyException implements Exception {
  SyncCipherMissingKeyException(this.message);

  final String message;

  @override
  String toString() => 'SyncCipherMissingKeyException: $message';
}

/// Thrown by a [SyncFieldCipher] when AEAD authentication fails: tampered
/// ciphertext, a wrong key, or a blob presented under the wrong binding
/// (transplanted onto another row, table, circle or key version).
///
/// This signals possible *tampering* and is reported at
/// [SyncEncryptionAlertSeverity.critical]. Storage handling is identical to
/// the missing-key case (ciphertext preserved, row locked, never crash) —
/// only the alert path differs.
class SyncCipherAuthException implements Exception {
  SyncCipherAuthException(this.message);

  final String message;

  @override
  String toString() => 'SyncCipherAuthException: $message';
}

/// What a [SyncEncryptionAlert] is about.
enum SyncEncryptionAlertKind {
  /// AEAD authentication failed on a blob — tampering, a transplanted blob,
  /// or corrupted ciphertext. Severity: critical.
  possibleTampering,

  /// A key needed to encrypt or decrypt is not (yet) available. The affected
  /// row is preserved (locked locally, or its push deferred). Severity: info.
  keyUnavailable,

  /// Shadow-mode parity check failed: the decrypted blob does not match the
  /// authoritative plaintext columns. Severity: warning.
  shadowParityMismatch,
}

/// Severity split for encryption alerts (tampering ≠ missing key): the app
/// maps these to its error reporting (e.g. Sentry levels).
enum SyncEncryptionAlertSeverity { info, warning, critical }

/// An encryption-related event the app should report. Never carries row
/// content — only identifiers.
class SyncEncryptionAlert {
  const SyncEncryptionAlert({
    required this.kind,
    required this.severity,
    required this.table,
    required this.rowId,
    required this.circleId,
    required this.keyVersion,
    required this.message,
  });

  final SyncEncryptionAlertKind kind;
  final SyncEncryptionAlertSeverity severity;

  /// Backend table name.
  final String table;
  final String rowId;
  final String? circleId;
  final int? keyVersion;

  /// Human-readable context. MUST NOT contain row content.
  final String message;

  @override
  String toString() =>
      'SyncEncryptionAlert(${severity.name}/${kind.name} '
      '$table/$rowId circle=$circleId v$keyVersion: $message)';
}

/// The encrypt/decrypt seam at the sync push/pull boundary.
///
/// The package owns *where* encryption happens (pop the registered content
/// fields out of the wire JSON on push, merge them back before `fromJson` on
/// pull); the app owns *how* (the AEAD construction, key custody, and mode
/// policy) by implementing this interface. The local database is never
/// encrypted — it stays plaintext so local queries keep working.
///
/// Implementations are responsible for:
/// * **AAD binding:** the blob must be cryptographically bound to
///   `(table, rowId, circleId, keyVersion)` so a malicious backend cannot
///   transplant a valid blob onto another row/table/circle/version. A binding
///   mismatch must throw [SyncCipherAuthException].
/// * **Blob self-versioning:** the encrypted payload must carry its own
///   schema version. SQL migrations cannot reach inside ciphertext, so the
///   cipher owns encrypted-field evolution: ignore unknown fields on read,
///   upgrade on write.
/// * **Typed failures:** throw [SyncCipherMissingKeyException] for
///   key-availability problems and [SyncCipherAuthException] for
///   authentication failures — the sync engine's severity-split alerting and
///   locked-row handling depend on the distinction.
abstract class SyncFieldCipher {
  /// Resolves the encryption mode for [circleId]'s rows in [table] at
  /// push/pull time. Called per row; implementations should answer from an
  /// in-memory cache.
  FutureOr<SyncEncryptionMode> modeFor({
    required String table,
    required String circleId,
  });

  /// Encrypts a row's content [fields] (already popped from the wire JSON)
  /// under the circle's active key.
  ///
  /// Throws [SyncCipherMissingKeyException] if no key is available for
  /// [circleId].
  Future<SyncEncryptedBlob> encryptContent({
    required String table,
    required String rowId,
    required String circleId,
    required Map<String, dynamic> fields,
  });

  /// Decrypts [contentEnc] (encrypted under [keyVersion]) back into the
  /// content-field map.
  ///
  /// Throws [SyncCipherMissingKeyException] if the key for [keyVersion] is
  /// not available, and [SyncCipherAuthException] on authentication failure.
  Future<Map<String, dynamic>> decryptContent({
    required String table,
    required String rowId,
    required String circleId,
    required String contentEnc,
    required int keyVersion,
  });
}
