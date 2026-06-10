import 'dart:convert';

import 'package:syncable/syncable.dart';

/// A deterministic [SyncFieldCipher] for seam tests.
///
/// Not real crypto — but it faithfully emulates the *contract*: the "blob"
/// embeds the AAD binding `(table, rowId, circleId, keyVersion)` and
/// decryption fails with [SyncCipherAuthException] when presented under a
/// different binding (the transplant/cut-and-paste case), or with
/// [SyncCipherMissingKeyException] when the circle/version is missing from
/// [keys]. The real construction (XChaCha20-Poly1305, ADR 0004) lives in the
/// app; the seam only depends on the contract.
class FakeFieldCipher implements SyncFieldCipher {
  /// Per-circle mode; circles not listed resolve to [defaultMode].
  final Map<String, SyncEncryptionMode> circleModes = {};
  SyncEncryptionMode defaultMode = SyncEncryptionMode.off;

  /// Key registry: circleId → key versions this client holds.
  final Map<String, Set<int>> keys = {};

  /// The version new blobs are encrypted under.
  int activeKeyVersion = 1;

  /// Row ids passed to [encryptContent] / [decryptContent], for asserting the
  /// cipher was (not) consulted.
  final List<String> encryptCalls = [];
  final List<String> decryptCalls = [];

  /// Artificial per-blob decrypt latency (keyed by the exact `contentEnc`
  /// string) — lets tests invert decode-completion order deterministically.
  final Map<String, Duration> blobDecryptDelays = {};

  String _aad(String table, String rowId, String circleId, int keyVersion) =>
      '$table|$rowId|$circleId|$keyVersion';

  /// Builds a blob exactly as [encryptContent] would — for fabricating
  /// backend rows in tests (including transplanted ones).
  String blobFor({
    required String table,
    required String rowId,
    required String circleId,
    required int keyVersion,
    required Map<String, dynamic> fields,
  }) {
    return base64Encode(
      utf8.encode(
        jsonEncode({
          'aad': _aad(table, rowId, circleId, keyVersion),
          'fields': fields,
        }),
      ),
    );
  }

  @override
  SyncEncryptionMode modeFor({
    required String table,
    required String circleId,
  }) {
    return circleModes[circleId] ?? defaultMode;
  }

  @override
  Future<SyncEncryptedBlob> encryptContent({
    required String table,
    required String rowId,
    required String circleId,
    required Map<String, dynamic> fields,
  }) async {
    encryptCalls.add(rowId);
    if (!(keys[circleId]?.contains(activeKeyVersion) ?? false)) {
      throw SyncCipherMissingKeyException(
        'No key v$activeKeyVersion for circle $circleId',
      );
    }
    return SyncEncryptedBlob(
      contentEnc: blobFor(
        table: table,
        rowId: rowId,
        circleId: circleId,
        keyVersion: activeKeyVersion,
        fields: fields,
      ),
      keyVersion: activeKeyVersion,
    );
  }

  @override
  Future<Map<String, dynamic>> decryptContent({
    required String table,
    required String rowId,
    required String circleId,
    required String contentEnc,
    required int keyVersion,
  }) async {
    decryptCalls.add(rowId);
    final delay = blobDecryptDelays[contentEnc];
    if (delay != null) await Future<void>.delayed(delay);
    if (!(keys[circleId]?.contains(keyVersion) ?? false)) {
      throw SyncCipherMissingKeyException(
        'No key v$keyVersion for circle $circleId',
      );
    }
    final Map<String, dynamic> decoded;
    try {
      decoded = (jsonDecode(utf8.decode(base64Decode(contentEnc))) as Map)
          .cast<String, dynamic>();
    } on FormatException {
      throw SyncCipherAuthException('Malformed blob');
    }
    if (decoded['aad'] != _aad(table, rowId, circleId, keyVersion)) {
      throw SyncCipherAuthException(
        'AAD mismatch: blob bound to ${decoded['aad']}, presented as '
        '${_aad(table, rowId, circleId, keyVersion)}',
      );
    }
    return (decoded['fields'] as Map).cast<String, dynamic>();
  }
}
