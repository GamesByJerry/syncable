import 'package:drift/drift.dart';
import 'package:syncable/src/syncable.dart';
import 'package:syncable/src/syncable_table.dart';

/// A [Syncable] whose table is registered for field encryption.
///
/// Adds the *locked-row* state: when a row arrives from the backend that
/// cannot be decrypted (missing key, unknown key version, or failed
/// authentication), it is still written locally — its content fields hold the
/// registration's placeholders, [locked] is `true`, and the verbatim
/// ciphertext is preserved in [lockedContentEnc] / [lockedKeyVersion] so the
/// content can be recovered *without a network round-trip* once key material
/// arrives ([SyncManager.retryLockedRows]). Discarding the blob instead would
/// lose the row's content permanently: the local cache is plaintext-only and
/// the row's `updated_at` watermark means incremental pull never re-fetches
/// it.
///
/// Like [Syncable.dirty], all three fields are local-only and MUST NOT be
/// included in `toJson` (the seam owns the wire representation of the
/// ciphertext) or populated from backend JSON in `fromJson` (the backend must
/// not be able to inject locked state). `toCompanion` MUST include them, so
/// that writing a decrypted row clears stale locked state.
///
/// The UI contract for locked rows: render a "locked" placeholder, and do not
/// allow editing content — a locked row's content fields are placeholders and
/// must never become a source for encryption. (The sync engine enforces this
/// on push by forwarding [lockedContentEnc] verbatim and nulling the content
/// fields, but the app should not offer the edit in the first place.)
abstract class EncryptedSyncable implements Syncable {
  /// Whether this row's content arrived undecryptable and its content fields
  /// hold placeholders.
  bool get locked;

  /// The verbatim `content_enc` blob of a locked row — preserved so
  /// decryption can be re-attempted locally. `null` unless [locked].
  String? get lockedContentEnc;

  /// The `key_version` the preserved blob was encrypted under. `null` unless
  /// [locked].
  int? get lockedKeyVersion;
}

/// A [SyncableTable] with the local-only locked-row fallback columns required
/// to register the table for field encryption.
///
/// Implementers should default [locked] to `false` and make the other two
/// columns nullable:
///
/// ```dart
/// @override
/// BoolColumn get locked => boolean().withDefault(const Constant(false))();
/// @override
/// TextColumn get lockedContentEnc => text().nullable()();
/// @override
/// IntColumn get lockedKeyVersion => integer().nullable()();
/// ```
abstract class EncryptedSyncableTable implements SyncableTable {
  BoolColumn get locked;
  TextColumn get lockedContentEnc;
  IntColumn get lockedKeyVersion;
}
