import 'dart:async';

import 'package:collection/collection.dart';
import 'package:drift/drift.dart';
import 'package:logging/logging.dart';
import 'package:supabase/supabase.dart';
import 'package:syncable/src/encrypted_syncable.dart';
import 'package:syncable/src/supabase_names.dart';
import 'package:syncable/src/sync_field_cipher.dart';
import 'package:syncable/src/sync_timestamp_storage.dart';
import 'package:syncable/src/syncable.dart';
import 'package:syncable/src/syncable_database.dart';
import 'package:syncable/src/syncable_table.dart';

/// The [SyncManager] is the main class for syncing data between a local Drift
/// database and a Supabase backend.
///
/// It handles the syncing of multiple tables and manages the state of the
/// syncing process. It also provides methods to register syncable tables and
/// to enable or disable syncing.
///
/// The [SyncManager] is designed to be used with the [SyncableDatabase]
/// class, which provides the local database functionality.
class SyncManager<T extends SyncableDatabase> {
  /// Creates a new [SyncManager] instance.
  ///
  /// The [localDatabase] parameter is required and must be an instance of
  /// [SyncableDatabase]. The [supabaseClient] parameter is also required and
  /// must be an instance of [SupabaseClient].
  ///
  /// The [syncInterval] parameter specifies the interval at which the
  /// internal sync loop runs. The sync loop is responsible for
  ///  - sending local changes to the backend,
  ///  - writing data received from the backend via a real-time subscription to
  ///    the local database.
  ///
  /// The [maxRows] parameter specifies the maximum number of rows that can be
  /// retrieved from the backend in one batch. Set this value to the one you
  /// configured in your Supabase dashboard or `supabase/config.toml`.
  ///
  /// The [syncTimestampStorage] parameter is optional and can be used to
  /// provide a custom implementation of [SyncTimestampStorage] for storing
  /// timestamps of the last sync operations. This can drastically reduce
  /// the amount of data that needs to be synced because only changed data
  /// since the last sync must be synced. Implementing a solution that persists
  /// the timestamps across app restarts (e.g. via shared preferences) is
  /// recommended.
  ///
  /// The [otherDevicesConsideredInactiveAfter] parameter specifies the
  /// duration after which other devices are considered inactive. This is used
  /// in combination with [lastTimeOtherDeviceWasActive] to determine whether
  /// other devices are currently active or not. A real-time subscription to
  /// the backend is only created if other devices are considered active.
  ///
  /// The [reconcileOverlap] parameter is the safety window subtracted from the
  /// last-pull watermark when reconciling from the backend. The reconcile only
  /// fetches metadata for rows changed since the previous pull (minus this
  /// overlap), which keeps the sweep cheap; the overlap absorbs clock skew
  /// between devices and rows written during the previous pull so none are
  /// missed. Requires a [syncTimestampStorage]; without one every reconcile
  /// falls back to a full sweep.
  ///
  /// The [fieldCipher] parameter enables the field-encryption seam: tables
  /// registered with a [SyncEncryption] config have their content fields
  /// folded into one encrypted blob per row at the push boundary and merged
  /// back at the pull boundary (the local database stays plaintext). The
  /// [onEncryptionAlert] callback receives encryption events the app should
  /// report — severities split tampering (critical) from key availability
  /// (info); see [SyncEncryptionAlert].
  SyncManager({
    required T localDatabase,
    required SupabaseClient supabaseClient,
    Duration syncInterval = const Duration(seconds: 1),
    int maxRows = 1000,
    SyncTimestampStorage? syncTimestampStorage,
    Duration otherDevicesConsideredInactiveAfter = const Duration(minutes: 2),
    Duration reconcileOverlap = const Duration(seconds: 10),
    SyncFieldCipher? fieldCipher,
    void Function(SyncEncryptionAlert alert)? onEncryptionAlert,
  }) : _localDb = localDatabase,
       _supabaseClient = supabaseClient,
       _syncInterval = syncInterval,
       _maxRows = maxRows,
       _syncTimestampStorage = syncTimestampStorage,
       _devicesConsideredInactiveAfter = otherDevicesConsideredInactiveAfter,
       _reconcileOverlap = reconcileOverlap,
       _fieldCipher = fieldCipher,
       _onEncryptionAlert = onEncryptionAlert,
       assert(
         syncInterval.inMilliseconds > 0,
         'Sync interval must be positive',
       );

  final _logger = Logger('syncable');

  final T _localDb;
  final SupabaseClient _supabaseClient;
  final SyncTimestampStorage? _syncTimestampStorage;
  final Duration _syncInterval;
  final int _maxRows;
  final Duration _devicesConsideredInactiveAfter;
  final Duration _reconcileOverlap;
  final SyncFieldCipher? _fieldCipher;
  final void Function(SyncEncryptionAlert alert)? _onEncryptionAlert;

  /// This is what gets set when [enableSync] gets called. Internally, whether
  /// the syncing is enabled or not is determined by [_syncingEnabled].
  bool __syncingEnabled = false;
  bool get syncingEnabled => __syncingEnabled;
  bool get _syncingEnabled =>
      __syncingEnabled && !_disposed && userId.isNotEmpty;

  /// Enables syncing for all registered syncables.
  ///
  /// This method will throw an exception if no syncables are registered.
  /// It will also start the sync loop, which will run in the background and
  /// handle the syncing of data between the local database and the backend.
  ///
  /// For any syncs to happen, the user ID must be set via [setUserId].
  void enableSync() {
    if (__syncingEnabled == true) return;

    if (_syncables.isEmpty) {
      throw Exception(
        'Failed to enable syncing because there are no registered syncables. '
        'Please register at least one syncable before enabling syncing.',
      );
    }

    __syncingEnabled = true;
    _startLoop();
    _onDependenciesChanged('syncing enabled');
  }

  /// Disables syncing to and from the backend.
  void disableSync() {
    if (__syncingEnabled == false) return;
    __syncingEnabled = false;
    _onDependenciesChanged('syncing disabled');
  }

  String _userId = '';
  String get userId => _userId;

  /// Sets the user ID for syncing. This is required for syncing to work.
  ///
  /// This must be the user ID of the currently logged in user. If the user ID is
  /// empty, syncing will be disabled and no data will be synced to or from
  /// the backend.
  void setUserId(String value) {
    if (_userId == value) return;
    _userId = value;
    _onDependenciesChanged("userId set to '$value'");
  }

  DateTime? _lastTimeOtherDeviceWasActive;
  DateTime? get lastTimeOtherDeviceWasActive => _lastTimeOtherDeviceWasActive;

  /// Sets the last time another device was active.
  ///
  /// This is used to determine whether other devices are currently active or
  /// not. A real-time subscription to the backend is only created if other
  /// devices are considered active.
  void setLastTimeOtherDeviceWasActive(DateTime? value) {
    if (_lastTimeOtherDeviceWasActive == value) return;
    _lastTimeOtherDeviceWasActive = value;
    _onDependenciesChanged('lastTimeOtherDeviceWasActive set to $value');
  }

  bool get isSyncingFromBackend =>
      _inQueues.values.any((queue) => queue.isNotEmpty);
  bool get isSyncingToBackend =>
      _outQueues.values.any((queue) => queue.isNotEmpty);

  bool _disposed = false;
  bool _loopRunning = false;

  /// Set while the sync loop is parked in [_idle]. Completed by [_wake] to drain
  /// immediately instead of waiting for the [_syncInterval] backstop timer.
  Completer<void>? _wakeSignal;

  final _syncables = <Type>[];
  List<Type> get syncables => _syncables;

  final Map<Type, TableInfo<SyncableTable, Syncable>> _localTables = {};
  final Map<Type, String> _backendTables = {};

  final Map<Type, Syncable Function(Map<String, dynamic>)> _fromJsons = {};
  final Map<Type, CompanionConstructor> _companions = {};

  final Map<Type, Set<Syncable>> _inQueues = {};
  final Map<Type, Map<String, Syncable>> _outQueues = {};

  final Map<Type, Set<Syncable>> _sentItems = {};
  final Map<Type, Set<Syncable>> _receivedItems = {};

  /// Ids of rows the backend has *permanently* rejected on push (e.g. a
  /// constraint or RLS violation). A poison row used to throw out of the whole
  /// table batch and wedge every other row's sync indefinitely; instead we
  /// isolate it here so the rest of the table — and every later table — keeps
  /// flushing. Quarantined rows are NOT deleted and their `dirty` flag is left
  /// set, so nothing is lost; this in-memory set just stops them being retried
  /// every loop (which would spin and flood error reporting). It is cleared on
  /// restart, so a fixed row gets another chance.
  ///
  /// Outgoing and incoming quarantines are SEPARATE on purpose: a row we can't
  /// *push* must still be *pullable*, so a backend fix or a newer tombstone from
  /// another device can land and supersede the stale local copy. Sharing one set
  /// would let an outbound-poison id suppress its own valid inbound update until
  /// restart.
  ///
  /// Each map is keyed id -> the `updatedAt` of the version that failed. A row is
  /// only skipped while the pending version is NOT newer than the quarantined
  /// one; a strictly-newer version (a local edit that may fix a push, or a
  /// remote fix/tombstone on pull) is let through to retry, and re-quarantined at
  /// its own version if it fails again. This stops a once-poison row from being
  /// stuck until restart when the underlying conflict is resolved.
  final Map<Type, Map<String, DateTime>> _outgoingQuarantined = {};
  final Map<Type, Map<String, DateTime>> _incomingQuarantined = {};

  /// Stuck-row detector (MC-424 §E): per table, id -> (version, consecutive
  /// reconcile sweeps the row has stayed dirty at that version). A row whose
  /// streak crosses [_stuckRowSweepThreshold] raises ONE severe — re-armed only
  /// if the row's `updatedAt` moves on — so a wedge that produces no other
  /// telemetry (e.g. an endless transient-retry loop) is still visible without
  /// flooding. Rows parked in the outgoing quarantine at the same version are
  /// exempt: their rejection already raised a severe when they were
  /// quarantined.
  final Map<Type, Map<String, ({DateTime version, int sweeps})>> _dirtyStreaks =
      {};

  static const int _stuckRowSweepThreshold = 5;

  /// Field-encryption registration per syncable (only set for tables that
  /// participate in the seam).
  final Map<Type, SyncEncryption> _encryption = {};

  /// Locked-blob instructions produced at the pull *decode* boundary, consumed
  /// at *write* time: a row that arrived undecryptable decodes into a
  /// placeholder model (the queues only carry [Syncable]s), and the verbatim
  /// ciphertext travels alongside in this sidecar, keyed by row id, to be
  /// written into the table's locked-row fallback columns together with the
  /// row. Entries are versioned on the row's `updatedAt` so a stale
  /// instruction is never applied to a different version's write.
  final Map<Type, Map<String, _PendingLockedBlob>> _pendingLockedBlobs = {};

  /// Rows whose push is deferred because the circle key needed to encrypt them
  /// is not available (enforced mode only). Kept out of the out-queue so they
  /// don't spin/alert every loop pass; re-enqueued by [retryLockedRows] when
  /// new key material arrives, by a newer local edit, or by a restart (rows
  /// stay dirty in the local db, so nothing is lost).
  final Map<Type, Map<String, Syncable>> _encryptionDeferred = {};

  final Map<Type, StreamSubscription<List<Syncable>>> _localSubscriptions = {};

  RealtimeChannel? _backendSubscription;
  bool get isSubscribedToBackend => _backendSubscription != null;

  /// Debounces rebuilding the backend channel after an unexpected drop so a
  /// persistently failing socket can't spin in a tight resubscribe loop.
  Timer? _resubscribeTimer;

  /// The number of items of type [syncable] that have been synced to the
  /// backend.
  int nSyncedToBackend(Type syncable) => _nSyncedToBackend[syncable] ??= 0;
  final Map<Type, int> _nSyncedToBackend = {};

  /// The number of items of type [syncable] that have been synced from the
  /// backend.
  int nSyncedFromBackend(Type syncable) => _nSyncedFromBackend[syncable] ??= 0;
  final Map<Type, int> _nSyncedFromBackend = {};

  int _nFullSyncs = 0;
  int get nFullSyncs => _nFullSyncs;

  void dispose() {
    _disposed = true;
    // Unpark the loop so it observes _disposed and exits promptly instead of
    // lingering until the backstop timer fires.
    _wake();
    _resubscribeTimer?.cancel();
    for (final subscription in _localSubscriptions.values) {
      subscription.cancel();
    }
    _backendSubscription?.unsubscribe();
  }

  /// Registers a syncable table with the sync manager.
  ///
  /// This method must be called before enabling syncing. It registers the
  /// table with the sync manager and sets up the necessary mappings for
  /// syncing data between the local database and the backend.
  ///
  /// The [backendTable] parameter specifies the name of the table on the
  /// backend. The [fromJson] parameter is used to convert the JSON data
  /// received from the backend into a [Syncable] object. The
  /// [companionConstructor] parameter is then used create a companion object
  /// from the [Syncable] object to write it to the local database.
  ///
  /// The [encryption] parameter opts the table into the field-encryption
  /// seam (requires a [SyncFieldCipher] on the manager): the registered
  /// content fields are folded into one encrypted blob per row on push and
  /// merged back on pull. See [SyncEncryption] for the requirements on the
  /// model, the table, and the backend schema.
  ///
  /// The generic type parameter must be provided and must be a  concrete
  /// subclass of [Syncable].
  void registerSyncable<S extends Syncable>({
    required String backendTable,
    required Syncable Function(Map<String, dynamic>) fromJson,
    required CompanionConstructor companionConstructor,
    SyncEncryption? encryption,
  }) {
    if (S == Syncable) {
      throw Exception(
        'Please provide the concrete type of your syncable class as a generic '
        'parameter. For example: `registerSyncable<MySyncable>(...)`',
      );
    }

    if (_loopRunning) {
      throw Exception(
        'Cannot register syncables after the sync manager was started',
      );
    }

    if (_syncables.contains(S)) return;

    final table = _localDb.getTable<S>();
    if (encryption != null) {
      _validateEncryptionRegistration<S>(
        backendTable: backendTable,
        encryption: encryption,
        table: table,
        companionConstructor: companionConstructor,
      );
    }

    _syncables.add(S);
    _localTables[S] = table;
    _backendTables[S] = backendTable;
    _fromJsons[S] = fromJson;
    _companions[S] = companionConstructor;
    if (encryption != null) _encryption[S] = encryption;
    _inQueues[S] = {};
    _outQueues[S] = {};
    _sentItems[S] = {};
    _receivedItems[S] = {};
    _outgoingQuarantined[S] = {};
    _incomingQuarantined[S] = {};
    _dirtyStreaks[S] = {};
    _pendingLockedBlobs[S] = {};
    _encryptionDeferred[S] = {};
  }

  /// Wire keys that the sync engine, RLS, or the backend itself must be able
  /// to read — they can never be folded into the encrypted blob. (The rule:
  /// anything sync/RLS needs to filter, order, join, or upsert on stays
  /// plaintext; everything else is content.)
  static const Set<String> _protectedWireKeys = {
    idKey,
    userIdKey,
    updatedAtKey,
    deletedKey,
    circleIdKey,
    contentEncKey,
    keyVersionKey,
  };

  void _validateEncryptionRegistration<S extends Syncable>({
    required String backendTable,
    required SyncEncryption encryption,
    required TableInfo<SyncableTable, S> table,
    required CompanionConstructor companionConstructor,
  }) {
    if (_fieldCipher == null) {
      throw Exception(
        'Cannot register encryption for $backendTable: no SyncFieldCipher was '
        'provided to the SyncManager',
      );
    }
    if (encryption.encryptedFields.isEmpty) {
      throw Exception(
        'Cannot register encryption for $backendTable with an empty '
        'encrypted-field set',
      );
    }
    final protected = encryption.encryptedFields.intersection(
      _protectedWireKeys,
    );
    if (protected.isNotEmpty) {
      throw Exception(
        'Cannot encrypt $protected of $backendTable: sync, RLS, or the seam '
        'itself depends on these staying plaintext',
      );
    }
    final orphanPlaceholders = encryption.lockedFieldPlaceholders.keys
        .toSet()
        .difference(encryption.encryptedFields);
    if (orphanPlaceholders.isNotEmpty) {
      throw Exception(
        'Locked-field placeholders $orphanPlaceholders of $backendTable do '
        'not match any registered encrypted field',
      );
    }
    final missingColumns = [
      'locked',
      'locked_content_enc',
      'locked_key_version',
    ].where((c) => !table.columnsByName.containsKey(c)).toList();
    if (missingColumns.isNotEmpty) {
      throw Exception(
        'Cannot register encryption for $backendTable: the local table is '
        'missing the locked-row fallback column(s) $missingColumns — '
        'implement EncryptedSyncableTable',
      );
    }
    if (companionConstructor is! EncryptedCompanionConstructor) {
      throw Exception(
        'Cannot register encryption for $backendTable: the companion '
        'constructor does not accept the locked-row columns — does the table '
        'implement EncryptedSyncableTable?',
      );
    }
  }

  Future<void> _startLoop() async {
    _loopRunning = true;
    _logger.info('Sync loop started');

    while (!_disposed) {
      try {
        for (final syncable in _syncables) {
          if (_disposed) break;
          await _processOutgoing(syncable);
        }
      } catch (e, s) {
        // coverage:ignore-start
        _logger.severe('Error processing outgoing: $e\n$s');
        // coverage:ignore-end
      }

      try {
        for (final syncable in _syncables) {
          if (_disposed) break;
          await _processIncoming(syncable);
        }
      } catch (e, s) {
        // coverage:ignore-start
        _logger.severe('Error processing incoming: $e\n$s');
        // coverage:ignore-end
      }

      if (_disposed) break;
      await _idle();
    }

    _loopRunning = false;
    _logger.info('Sync loop stopped');
  }

  /// Wakes the sync loop if it is currently parked in [_idle]. A no-op when the
  /// loop is busy (no pending signal): the freshly enqueued work is already in a
  /// queue, so the loop's next [_idle] queue check drains it without a signal.
  void _wake() {
    final signal = _wakeSignal;
    if (signal != null && !signal.isCompleted) {
      signal.complete();
    }
  }

  /// Parks the loop until there is work to do. Returns immediately if a queue
  /// already holds items — this closes the missed-wake race where an item is
  /// enqueued between a drain finishing and the loop parking. Otherwise it waits
  /// for either a [_wake] signal (sub-second steady-state latency) or the
  /// [_syncInterval] backstop timer, whichever fires first.
  Future<void> _idle() async {
    if (_disposed) return;
    if (isSyncingToBackend || isSyncingFromBackend) return;

    final signal = _wakeSignal = Completer<void>();
    final backstop = Timer(_syncInterval, () {
      if (!signal.isCompleted) signal.complete();
    });

    try {
      await signal.future;
    } finally {
      backstop.cancel();
      if (identical(_wakeSignal, signal)) _wakeSignal = null;
    }
  }

  /// Goes through the local tables for all registered syncables and sets the
  /// user ID to the currently set [userId] for all entries that don't have
  /// a user ID yet.
  ///
  /// This is useful if you support anonymous usage of your app. You can first
  /// write items to the local database without setting the user ID until
  /// the user registers or logs in. Afterwards, you can call
  /// this method to set the user ID and sync the data to the backend (requires
  /// syncing to be enabled via [enableSync]).
  Future<void> fillMissingUserIdForLocalTables() async {
    if (userId.isEmpty) {
      _logger.warning(
        'Not setting user ID for local tables because user ID is empty',
      );
      return;
    }

    final tables = _syncables.map((s) => _localTables[s]!).toList();
    final companions = List<UpdateCompanion<Syncable>>.from(
      _syncables.map((s) => _companions[s]!(userId: Value(userId))),
    );

    await _localDb.transaction(() async {
      for (int i = 0; i < tables.length; i++) {
        await (_localDb.update(
          tables[i],
        )..where((row) => row.userId.isNull())).write(companions[i]);
      }
    });
  }

  /// Matches a backend-valid (RFC 4122) UUID, the shape Supabase requires for a
  /// `user_id`. Anything else (e.g. an offline-guest id `offline_guest_<uuid>`)
  /// can never be accepted by the backend.
  static final RegExp _uuidPattern = RegExp(
    '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-'
    '[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\$',
  );

  /// Hard-deletes local rows whose [Syncable.userId] is not a backend-valid
  /// UUID, returning the number of rows removed.
  ///
  /// Such rows can never sync: Supabase rejects a non-UUID `user_id` with a
  /// `22P02` error, which jams the outgoing push queue and stalls all sync. The
  /// canonical source is offline-guest data (`offline_guest_<uuid>`) left behind
  /// when a real user later takes over the same install. Because the backend
  /// never accepted these rows, removing them needs no soft-delete tombstone —
  /// a hard delete is correct and final.
  ///
  /// Rows with a null `userId` are left untouched (use
  /// [fillMissingUserIdForLocalTables] to adopt those into the current user).
  /// Call this after a real (UUID) user signs in, before re-enabling sync.
  Future<int> purgeNonUuidOwnedRows() async {
    int removed = 0;
    await _localDb.transaction(() async {
      for (final syncable in _syncables) {
        final table = _localTables[syncable]!;
        // Load only owned rows. Null-owner rows are never purged here — they are
        // adopted into the current user by [fillMissingUserIdForLocalTables] —
        // and excluding them in SQL avoids pulling every local row into memory.
        final rows = await (_localDb.select(
          table,
        )..where((row) => row.userId.isNotNull())).get();
        final doomedIds = rows
            .where((r) => !_uuidPattern.hasMatch(r.userId!))
            .map((r) => r.id)
            .toList();
        if (doomedIds.isEmpty) continue;
        // Chunk to stay under SQLite's bound-variable limit (~999), the same
        // guard _batchWriteIncoming uses for its id-keyed deletes.
        for (final idChunk in doomedIds.slices(500)) {
          await (_localDb.delete(
            table,
          )..where((row) => row.id.isIn(idChunk))).go();
        }
        // Drop any copies already sitting in the out queue so a pending push
        // can't resend the non-UUID owner and re-jam the queue with 22P02 (the
        // MC-380 failure mode) even though the local row is now gone.
        final doomedSet = doomedIds.toSet();
        _outQueues[syncable]?.removeWhere((id, _) => doomedSet.contains(id));
        removed += doomedIds.length;
      }
    });
    if (removed > 0) {
      _logger.info('Purged $removed non-UUID-owned local row(s)');
    }
    return removed;
  }

  /// Re-attempts decryption of every locked row from its locally preserved
  /// ciphertext, and re-enqueues every dirty row of encrypted tables whose
  /// push may have been deferred for a missing circle key. Call this whenever
  /// new key material lands (a circle key wrap arrives for this user) AND
  /// once after sign-in/key bootstrap on startup: deferral state is
  /// in-memory, and a deferred row's `updatedAt` may sit behind the persisted
  /// push watermark, so the regular local-change sweep alone would never
  /// retry it. No network round-trip is involved in unlocking — the
  /// ciphertext was preserved exactly for this moment.
  ///
  /// Unlocking is not a local edit: the row's `updatedAt` and `dirty` flag are
  /// left as they were, so an unlock never causes a push and never disturbs
  /// conflict resolution. Rows whose key is still missing stay locked
  /// silently; rows whose blob fails authentication stay locked and are
  /// reported as possible tampering.
  ///
  /// Returns the number of rows unlocked.
  Future<int> retryLockedRows() async {
    final cipher = _fieldCipher;
    if (cipher == null) return 0;

    var unlocked = 0;

    for (final syncable in _syncables) {
      final encryption = _encryption[syncable];
      if (encryption == null) continue;

      final table = _localTables[syncable]!;
      final backendTable = _backendTables[syncable]!;
      final lockedColumn =
          table.columnsByName['locked']! as GeneratedColumn<bool>;

      final lockedRows = await (_localDb.select(
        table,
      )..where((_) => lockedColumn.equals(true))).get();

      for (final row in lockedRows) {
        final lockedRow = row as EncryptedSyncable;
        final contentEnc = lockedRow.lockedContentEnc;
        final keyVersion = lockedRow.lockedKeyVersion;
        final json = row.toJson();
        final circleId = json[circleIdKey] as String?;
        if (contentEnc == null || keyVersion == null || circleId == null) {
          continue;
        }

        try {
          final fields = await cipher.decryptContent(
            table: backendTable,
            rowId: row.id,
            circleId: circleId,
            contentEnc: contentEnc,
            keyVersion: keyVersion,
          );
          fields.removeWhere((key, _) => _protectedWireKeys.contains(key));
          json.addAll(fields);
          final item = _fromJsons[syncable]!(json);

          await _localDb.transaction(() async {
            // The decoded model carries the real content plus cleared locked
            // state; its companion rewrites the row in place.
            await (_localDb.update(
              table,
            )..where((tbl) => tbl.id.equals(row.id))).write(item.toCompanion());
            // toCompanion defaults dirty=true. Restore the row's original
            // flag: an unlock is not a local edit (no re-push), but a pending
            // local write (e.g. a tombstone) must survive the unlock.
            final dirtyCompanion =
                _companions[syncable]!(dirty: Value(row.dirty))
                    as UpdateCompanion<Syncable>;
            await (_localDb.update(
              table,
            )..where((tbl) => tbl.id.equals(row.id))).write(dirtyCompanion);
          });
          unlocked++;
        } on SyncCipherMissingKeyException {
          _logger.fine(
            'Row ${row.id} in $backendTable stays locked '
            '(key v$keyVersion still unavailable)',
          );
        } on SyncCipherAuthException catch (e) {
          _emitAlert(
            SyncEncryptionAlert(
              kind: SyncEncryptionAlertKind.possibleTampering,
              severity: SyncEncryptionAlertSeverity.critical,
              table: backendTable,
              rowId: row.id,
              circleId: circleId,
              keyVersion: keyVersion,
              message:
                  'Preserved blob failed authentication on unlock retry: $e',
            ),
          );
        }
      }
    }

    // New key material may also unblock pushes deferred in enforced mode.
    // Re-enqueue from the DATABASE, not the in-memory deferral map: deferral
    // state does not survive a restart, but the rows stay persistently dirty
    // — and the persisted push watermark may have advanced past their
    // updatedAt (any later successful push does that), in which case the
    // normal local-change sweep would filter them out forever. Enqueueing
    // directly bypasses that filter; pushes are idempotent upserts, so
    // re-enqueueing a dirty row that was about to push anyway is harmless.
    var reenqueued = false;
    for (final syncable in _syncables) {
      if (_encryption[syncable] == null) continue;
      _encryptionDeferred[syncable]!.clear();

      final table = _localTables[syncable]!;
      final dirtyColumn =
          table.columnsByName['dirty']! as GeneratedColumn<bool>;
      final dirtyRows = await (_localDb.select(
        table,
      )..where((_) => dirtyColumn.equals(true))).get();
      if (dirtyRows.isEmpty) continue;

      final outQueue = _outQueues[syncable]!;
      for (final row in dirtyRows) {
        outQueue[row.id] = row;
      }
      reenqueued = true;
    }
    if (reenqueued) _wake();

    if (unlocked > 0) {
      _logger.info('Unlocked $unlocked locked row(s) after key arrival');
    }
    return unlocked;
  }

  /// Marks every non-locked row of [circleId] in encryption-registered tables
  /// for re-push — the decrypt-and-restore direction: with the circle's mode
  /// resolved back to [SyncEncryptionMode.off] (do that BEFORE calling), the
  /// re-pushes rewrite the backend's plaintext columns from the locally
  /// decrypted copies and null the blob columns, dropping the circle back out
  /// of encryption.
  ///
  /// Each row's `updatedAt` is bumped (backends reject non-newer writes), so
  /// other devices re-pull the circle afterwards. Locked rows are skipped —
  /// this device cannot restore content it could never read; run
  /// [retryLockedRows] first if the key arrived late.
  ///
  /// Returns the number of rows marked for re-push.
  Future<int> repushRowsForRestore({required String circleId}) async {
    var marked = 0;
    for (final syncable in _syncables) {
      if (_encryption[syncable] == null) continue;
      final table = _localTables[syncable]!;

      // Filter by circle in SQL so only the circle's rows are loaded, not the
      // whole table. A registered table without a circle_id column cannot
      // hold circle-scoped rows at all.
      final circleIdColumn =
          table.columnsByName[circleIdKey] as GeneratedColumn<String>?;
      if (circleIdColumn == null) continue;

      final rows = await (_localDb.select(
        table,
      )..where((_) => circleIdColumn.equals(circleId))).get();
      for (final row in rows) {
        if (row is EncryptedSyncable && row.locked) {
          _logger.warning(
            'Skipping locked row ${row.id} of ${_backendTables[syncable]} '
            'during restore of circle $circleId — its content was never '
            'decryptable on this device',
          );
          continue;
        }
        final companion =
            _companions[syncable]!(
                  dirty: const Value(true),
                  updatedAt: Value(DateTime.now().toUtc()),
                )
                as UpdateCompanion<Syncable>;
        await (_localDb.update(
          table,
        )..where((tbl) => tbl.id.equals(row.id))).write(companion);
        marked++;
      }
    }
    if (marked > 0) {
      _logger.info(
        'Marked $marked row(s) of circle $circleId for plaintext restore',
      );
    }
    return marked;
  }

  /// Marks every non-locked row of [circleId] in encryption-registered
  /// tables for re-push WITHOUT bumping `updated_at` — the re-encrypt sweep
  /// direction (MC-430): after a promotion to enforced mode the re-pushes
  /// null the backend's plaintext columns and attach the blob, and after a
  /// key rotation they rewrite each row's blob under the new active key. In
  /// both cases the *content* is unchanged, so `updated_at` must stay put:
  /// other devices must not re-pull the circle, and a sweep re-push must
  /// never win an LWW race against a genuine concurrent edit. Callers should
  /// reconcile (pull) first to minimise the window where a re-push rewrites
  /// a newer backend row this device has not pulled yet.
  ///
  /// Locked rows are skipped: their content fields are placeholders, never a
  /// source for encryption — run [retryLockedRows] first if their key has
  /// arrived.
  ///
  /// Rows are enqueued directly as well as marked dirty: their unchanged
  /// `updated_at` typically sits behind the persisted push watermark, so the
  /// regular local-change sweep alone would never pick them up (the same
  /// reasoning as the deferred-push re-enqueue in [retryLockedRows]).
  ///
  /// **Backend requirement:** because `updated_at` is unchanged, the
  /// backend's upsert path must accept a SAME-timestamp rewrite of an
  /// existing row. A backend that discards non-newer updates (e.g. a
  /// `discard_older_updates`-style trigger comparing
  /// `NEW.updated_at <= OLD.updated_at`) silently drops these re-pushes and
  /// the sweep can never converge — exempt same-timestamp updates (or the
  /// crypto columns) in such a trigger before using this.
  ///
  /// Returns the number of rows marked for re-push.
  Future<int> repushRowsForReencrypt({required String circleId}) async {
    var marked = 0;
    for (final syncable in _syncables) {
      if (_encryption[syncable] == null) continue;
      final table = _localTables[syncable]!;

      final circleIdColumn =
          table.columnsByName[circleIdKey] as GeneratedColumn<String>?;
      if (circleIdColumn == null) continue;

      final rows = await (_localDb.select(
        table,
      )..where((_) => circleIdColumn.equals(circleId))).get();
      final toMark = [
        for (final row in rows)
          if (row is! EncryptedSyncable || !row.locked) row,
      ];
      if (toMark.isEmpty) continue;

      // One batch per table: a rotation sweep can touch thousands of rows,
      // and per-row updates would mean thousands of individual commits.
      final companion =
          _companions[syncable]!(dirty: const Value(true))
              as UpdateCompanion<Syncable>;
      await _localDb.batch((batch) {
        for (final row in toMark) {
          batch.update(
            table,
            companion,
            where: (tbl) => tbl.id.equals(row.id),
          );
        }
      });
      final outQueue = _outQueues[syncable]!;
      for (final row in toMark) {
        outQueue[row.id] = row;
      }
      marked += toMark.length;
    }
    if (marked > 0) {
      _wake();
      _logger.info(
        'Marked $marked row(s) of circle $circleId for re-encrypt re-push',
      );
    }
    return marked;
  }

  Future _onDependenciesChanged(String reason) async {
    _maybeSubscribeToLocalChanges();
    _maybeSubscribeToBackendChanges();

    // Fire-and-forget callers (enableSync / setUserId / device-active) drop
    // this Future, so a throwing sweep would otherwise become an UNHANDLED
    // async error in the host app — invisible as a sync failure. Contain it
    // here; _syncTables already logged a severe naming the failing table. The
    // explicit [syncTables] API still propagates to its caller.
    try {
      await _syncTables(reason);
    } catch (_) {
      // Logged at the failure site with table attribution.
    }
  }

  void _maybeSubscribeToLocalChanges() {
    _clearLocalSubscriptions();

    // Expected while signed out — INFO, not a Sentry-captured warning.
    if (!_syncingEnabled) {
      _logger.info(
        'Not subscribed to local changes because syncing is disabled',
      );
      return;
    }

    if (userId.isEmpty) {
      _logger.info('Not subscribed to local changes because user ID is empty');
      return;
    }

    for (final syncable in _syncables) {
      _localSubscriptions[syncable] = _localDb.subscribe(
        table: _localTables[syncable]!,
        // GAM-389: RLS-trusting — watch ALL local rows, not just this user's,
        // so a co-member-owned row still pushes when locally edited.
        filter: (SyncableTable row) => const Constant<bool>(true),
        onChange: (rows) {
          if (_syncingEnabled) {
            _pushLocalChangesToOutQueue(syncable, rows.cast());
          }
        },
      );
    }

    _logger.info('Subscribed to local changes');
  }

  /// Returns the number of rows enqueued (for sweep outcome logging).
  int _pushLocalChangesToOutQueue(Type syncable, Iterable<Syncable> rows) {
    final outQueue = _outQueues[syncable]!;
    final receivedItems = _receivedItems[syncable]!;

    bool updateHasNotBeenSentYet(Syncable row) =>
        row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0)) &&
        row.updatedAt.isAfter(_lastPushedTimestamp(syncable) ?? DateTime(0));

    var enqueued = 0;
    for (final row
        in rows
            // Only push rows with unpushed LOCAL changes. A row pulled from the
            // backend is written with dirty=false, so it is never echoed back —
            // even across an app restart (the in-memory receivedItems set does
            // not survive that, the persistent dirty column does). This is what
            // stops non-owner co-member rows re-pushing and failing RLS.
            .where((r) => r.dirty)
            .where((r) => !receivedItems.contains(r))
            .where(updateHasNotBeenSentYet)) {
      outQueue[row.id] = row;
      enqueued++;
    }

    // Drain the new work now rather than on the next backstop tick. Reached from
    // the local-change Drift subscription (writer side) and from _syncTable.
    if (enqueued > 0) _wake();
    return enqueued;
  }

  /// MC-424 §E stuck-row detector: called once per reconcile sweep per table
  /// (NOT from the fast wake loop) with the full local row set. A row that
  /// stays dirty at the same version across [_stuckRowSweepThreshold]
  /// consecutive sweeps means pushes are not succeeding and nothing else may
  /// be saying so (e.g. an endless transient-retry loop) — raise one severe
  /// exactly when the streak crosses the threshold.
  void _trackStuckRows(Type syncable, Iterable<Syncable> rows) {
    final streaks = _dirtyStreaks[syncable]!;
    final quarantined = _outgoingQuarantined[syncable]!;

    final dirtyNow = {
      for (final r in rows.where((r) => r.dirty)) r.id: r.updatedAt,
    };

    // A row that pushed (or was deleted) since last sweep ends its streak.
    streaks.removeWhere((id, _) => !dirtyNow.containsKey(id));

    dirtyNow.forEach((id, version) {
      // Quarantined at this version: already reported once, and parked on
      // purpose — counting it again would double-report every wedge.
      final quarantinedAt = quarantined[id];
      if (quarantinedAt != null && !version.isAfter(quarantinedAt)) {
        streaks.remove(id);
        return;
      }

      final prev = streaks[id];
      if (prev == null || prev.version != version) {
        // New dirty row, or a local edit moved the version on: (re)arm.
        streaks[id] = (version: version, sweeps: 1);
        return;
      }

      final sweeps = prev.sweeps + 1;
      streaks[id] = (version: version, sweeps: sweeps);
      if (sweeps == _stuckRowSweepThreshold) {
        _logger.severe(
          'Stuck row: $id in ${_backendTables[syncable]} still dirty after '
          '$sweeps sweeps without a successful push',
        );
      }
    });
  }

  void _maybeSubscribeToBackendChanges() {
    final otherDevicesActive = _otherDevicesActive();

    if (!_syncingEnabled || !otherDevicesActive) {
      if (_backendSubscription != null) {
        _backendSubscription?.unsubscribe();
        _backendSubscription = null;
      }

      String reason;
      var expected = true;

      if (!__syncingEnabled) {
        reason = 'syncing is disabled';
      } else if (userId.isEmpty) {
        reason = 'the user ID is empty';
      } else if (!otherDevicesActive) {
        reason = 'no other devices are active';
      } else {
        reason = '... good question. Please file an issue';
        expected = false;
      }

      // The known reasons are normal states (signed out, solo device) — INFO
      // breadcrumbs, not Sentry captures. Only the can't-happen fallback warns.
      _logger.log(
        expected ? Level.INFO : Level.WARNING,
        'Not subscribed to backend changes because $reason',
      );

      return;
    }

    if (_backendSubscription != null) {
      return;
    }

    final channel = _supabaseClient.channel('backend_changes');
    _backendSubscription = channel;

    for (final syncable in _syncables) {
      channel.onPostgresChanges(
        schema: publicSchema,
        table: _backendTables[syncable],
        event: PostgresChangeEvent.all,
        callback: (p) {
          if (_disposed) return;
          if (p.newRecord.isNotEmpty) {
            // Decode (and, for encrypted tables, decrypt) off the callback;
            // _enqueueIncoming wakes the loop once the item is queued — the
            // reader-side half of the sub-second path.
            unawaited(_enqueueIncoming(syncable, p.newRecord));
          }
        },
        // GAM-389: no user_id filter — Postgres Changes only delivers rows the
        // client may read under RLS, which already scopes to the user's circles.
      );
    }

    channel.subscribe(
      (status, error) => _onBackendSubscriptionStatus(channel, status, error),
    );
  }

  /// Handles backend channel lifecycle (MC-413 item 6). Every (re)connect forces
  /// a reconcile to backfill events the channel could not replay while it was
  /// down; an unexpected drop rebuilds the channel, because a closed realtime
  /// channel cannot be re-subscribed in place.
  void _onBackendSubscriptionStatus(
    RealtimeChannel channel,
    RealtimeSubscribeStatus status,
    Object? error,
  ) {
    switch (status) {
      case RealtimeSubscribeStatus.subscribed:
        _resubscribeTimer?.cancel();
        _logger.info('Subscribed to backend changes');
        // Backfill whatever realtime could not deliver while (re)connecting.
        // Fire-and-forget, but guard so a failed reconcile can't surface as an
        // unhandled async error from this realtime callback.
        unawaited(
          syncTables().catchError((Object e, StackTrace s) {
            _logger.severe('Backfill on (re)connect failed: $e\n$s');
          }),
        );
      case RealtimeSubscribeStatus.closed:
      case RealtimeSubscribeStatus.channelError:
      case RealtimeSubscribeStatus.timedOut:
        _logger.warning(
          'Backend subscription dropped ($status)'
          '${error != null ? ': $error' : ''}',
        );
        _handleBackendSubscriptionDrop(channel);
    }
  }

  void _handleBackendSubscriptionDrop(RealtimeChannel channel) {
    // Ignore late callbacks from a channel we already replaced or tore down.
    if (!identical(channel, _backendSubscription)) return;
    _backendSubscription = null;
    // Kill the dead channel's postgres callbacks and its own rejoin attempts so
    // we never end up with two live channels delivering duplicate events.
    channel.unsubscribe();
    if (_disposed) return;
    // Rebuild on a backstop-paced debounce. _maybeSubscribeToBackendChanges
    // re-checks the want-conditions, so an intentional teardown (sync disabled /
    // no active devices) simply won't resubscribe.
    _scheduleBackendResubscribe();
  }

  void _scheduleBackendResubscribe() {
    if (_disposed) return;
    if (_resubscribeTimer?.isActive ?? false) return;
    _resubscribeTimer = Timer(_syncInterval, () {
      if (_disposed) return;
      _maybeSubscribeToBackendChanges();
    });
  }

  /// Syncs all tables registered with the sync manager.
  ///
  /// This method is called automatically when the sync manager is started,
  /// or when dependencies change (e.g. user ID, last active time of other
  /// devices, ...).
  ///
  /// It can also be called manually to force a sync (still requires syncing
  /// to be enabled via [enableSync]).
  ///
  /// Set [fullResync] to force a FULL sweep that ignores the incremental
  /// last-pull watermark (and the "skip if no other device was active" guard).
  /// The normal reconcile only asks the backend for rows changed since the last
  /// pull, which is unsound when the set of rows the client may read GROWS for a
  /// non-temporal reason — e.g. gaining access to a row (via RLS) whose
  /// `updatedAt` predates that watermark. Such rows are never newer than the
  /// watermark, so an incremental sweep skips them indefinitely; a full resync
  /// pulls them. Call this right after any membership/permission change that can
  /// widen visibility. The watermark is still advanced afterwards, so subsequent
  /// reconciles return to being incremental.
  Future<void> syncTables({bool fullResync = false}) async {
    await _syncTables('Manual sync', fullResync: fullResync);
  }

  Future<void> _syncTables(String reason, {bool fullResync = false}) async {
    // Expected idle states (signed out / pre-login), not faults: INFO so they
    // stay visible as breadcrumbs without becoming a Sentry capture on every
    // signed-out launch (MC-424 §E).
    if (!__syncingEnabled) {
      _logger.info('Tables not getting synced because syncing is disabled');
      return;
    }

    if (userId.isEmpty) {
      _logger.info('Tables not getting synced because user ID is empty');
      return;
    }

    // The identity matters for forensics: watermarks are stored per-user, so a
    // sweep running under an unexpected user ID explains "fetched nothing".
    _logger.info('Syncing all tables. Reason: $reason (user: $_userId)');

    for (final syncable in _syncables) {
      try {
        await _syncTable(syncable, fullResync: fullResync);
      } catch (e, s) {
        // Name the table before propagating: app-side this otherwise surfaces
        // as a bare "reconcile failed" with no attribution (MC-424 §E).
        _logger.severe(
          'Sweep failed at table ${_backendTables[syncable]}',
          e,
          s,
        );
        rethrow;
      }
    }

    _nFullSyncs++;
  }

  Future<void> _syncTable(Type syncable, {bool fullResync = false}) async {
    final table = _backendTables[syncable];

    if (!_syncingEnabled) {
      _logger.info('Sweep $table: skipped (syncing disabled)');
      return;
    }

    final localItems = await _localDb.select(_localTables[syncable]!).get();

    // Check after async gap
    if (!_syncingEnabled) {
      _logger.info('Sweep $table: skipped (syncing disabled mid-sweep)');
      return;
    }

    assert(_userId.isNotEmpty);
    // GAM-389: push ALL local rows, not just this user's. The updatedAt /
    // receivedItems dedup below still guards against echoing pulled rows.
    final queued = _pushLocalChangesToOutQueue(syncable, localItems);

    _trackStuckRows(syncable, localItems);

    // A forced full resync must always pull: the caller is widening visibility
    // (e.g. a just-joined circle), so the "no other device was active" shortcut
    // — which is only about avoiding redundant pulls of unchanged data — must
    // not suppress it.
    if (!fullResync && _skipSyncFromBackend(syncable)) {
      _logger.info(
        'Sweep $table: queued $queued for push, pull skipped '
        '(no other device active since last sync)',
      );
      return;
    }

    final localItemsUpdatedAt = {for (final i in localItems) i.id: i.updatedAt};

    assert(_userId.isNotEmpty);
    // The watermark the metadata fetch will filter on — logged with the
    // outcome so a sweep that "fetched nothing" is diagnosable (was it a full
    // sweep, or incremental against a watermark that's ahead of the data?).
    final watermark = fullResync
        ? null
        : _lastPulledTimestamp(syncable)?.toUtc();
    // Capture the watermark BEFORE fetching: any row written while this pull is
    // in flight has updatedAt >= pullStartedAt, so the next reconcile's
    // `> pullStartedAt - overlap` filter still catches it.
    final pullStartedAt = DateTime.now().toUtc();
    final backendItems = await _fetchBackendItemMetadata(
      syncable,
      fullResync: fullResync,
    );

    final itemsToPull = _getItemsToPullFromBackend(
      backendItems,
      localItemsUpdatedAt,
    );

    // Use batches because all the UUIDs make the URI become too long otherwise.
    for (final batch in itemsToPull.slices(100)) {
      if (!_syncingEnabled) {
        _logger.info('Sweep $table: aborted (syncing disabled mid-pull)');
        return;
      }
      final pulledBatch = await _supabaseClient
          .from(_backendTables[syncable]!)
          .select()
          // GAM-389: no user_id filter — pull whatever RLS permits.
          .inFilter(idKey, batch);

      // Decode the wire rows (decrypting registered content) concurrently —
      // ids within a batch are unique, so completion order cannot reorder
      // versions of a row. _enqueueIncoming contains per-row failures and
      // wakes the loop so an off-loop reconcile's finds get written promptly.
      await Future.wait(
        pulledBatch.map((wireRow) => _enqueueIncoming(syncable, wireRow)),
      );
    }

    await _updateLastPulledTimeStamp(syncable, pullStartedAt);

    // One outcome line per table per sweep — an empty sweep must still say it
    // ran and what it saw (MC-424 §E: six silent empty sweeps hid a multi-day
    // stranded-cache outage).
    _logger.info(
      'Sweep $table: queued $queued for push, pulled ${itemsToPull.length} '
      'of ${backendItems.length} backend candidates '
      '(watermark: ${watermark?.toIso8601String() ?? 'none — full sweep'})',
    );
  }

  bool _skipSyncFromBackend(Type syncable) {
    final lastSyncFromBackend = _lastPulledTimestamp(syncable);

    // If no device was active since our last sync from backend, we don't need
    // to sync again.
    if (lastSyncFromBackend != null &&
        lastTimeOtherDeviceWasActive != null &&
        lastSyncFromBackend.isAfter(
          lastTimeOtherDeviceWasActive!.add(_devicesConsideredInactiveAfter),
        )) {
      return true;
    }

    return false;
  }

  /// Retrieves the IDs and `lastUpdatedAt` timestamps for all rows of a
  /// syncable in the backend. These can be used to determine which items need
  /// to be synced from the backend.
  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(
    Type syncable, {
    bool fullResync = false,
  }) async {
    final List<Map<String, dynamic>> backendItems = [];

    // MC-413 item 4: incremental sweep. Once we have a watermark from a previous
    // pull, only ask the backend for rows changed since then (minus the overlap
    // window), turning an O(all rows) sweep into O(rows changed since last
    // pull). A null watermark — first pull, or no timestamp storage — falls back
    // to a full sweep so the initial reconcile never misses anything.
    // Force UTC: the watermark comes from a pluggable SyncTimestampStorage that
    // may hand back a local DateTime, and toIso8601String() on a local time
    // omits the 'Z' the backend needs — a silent timezone mismatch in the
    // server-side `updated_at >` filter.
    // A forced full resync deliberately discards the watermark so the sweep is
    // unbounded — see [syncTables]'s `fullResync`. Rows that became newly
    // readable but predate the watermark are only caught by an unbounded sweep.
    final lastPulled = fullResync
        ? null
        : _lastPulledTimestamp(syncable)?.toUtc();
    final changedSince = lastPulled?.subtract(_reconcileOverlap);

    int offset = 0;
    bool hasMore = true;

    while (hasMore && _syncingEnabled) {
      var query = _supabaseClient
          .from(_backendTables[syncable]!)
          .select('$idKey,$updatedAtKey');
      // GAM-389: no user_id filter — metadata for all RLS-visible rows.
      if (changedSince != null) {
        query = query.gt(updatedAtKey, changedSince.toIso8601String());
      }

      final batch = await query
          .range(offset, offset + _maxRows - 1)
          // Use consistent ordering to prevent duplicates
          .order(idKey, ascending: true);

      backendItems.addAll(batch);
      hasMore = batch.length == _maxRows;
      offset += _maxRows;

      if (batch.isNotEmpty) {
        _logger.info(
          'Fetched batch of ${batch.length} metadata items for table '
          "'${_backendTables[syncable]!}', total so far: ${backendItems.length}",
        );
      }
    }

    return backendItems;
  }

  Iterable<String> _getItemsToPullFromBackend(
    List<Map<String, dynamic>> backendItems,
    Map<String, DateTime> localItemsUpdatedAt,
  ) {
    bool needsPulling(String itemId, DateTime backendItemUpdatedAt) =>
        localItemsUpdatedAt[itemId] == null ||
        backendItemUpdatedAt.isAfter(localItemsUpdatedAt[itemId]!);

    return backendItems
        .where(
          (backendItem) => needsPulling(
            backendItem[idKey]! as String,
            DateTime.parse(backendItem[updatedAtKey]! as String),
          ),
        )
        .map((backendItem) => backendItem[idKey]! as String);
  }

  /// Decodes a backend wire row (the pull half of the encryption seam) and
  /// enqueues it for the local write. Decode failures are contained per row so
  /// one malformed row cannot take down a realtime callback or a whole batch.
  Future<void> _enqueueIncoming(
    Type syncable,
    Map<String, dynamic> wireRow,
  ) async {
    final Syncable item;
    try {
      item = await _decodeIncoming(syncable, wireRow);
    } catch (e, s) {
      // coverage:ignore-start
      _logger.severe(
        'Failed to decode incoming row for table '
        '${_backendTables[syncable]}: $e\n$s',
      );
      return;
      // coverage:ignore-end
    }
    if (_disposed) return;
    _inQueues[syncable]!.add(item);
    _wake();
  }

  /// Turns a backend wire row into a [Syncable] — for encrypted tables this is
  /// where the blob is decrypted and the content fields merged back before
  /// `fromJson`. Conflict resolution never sees ciphertext: it runs on the
  /// decoded item's plaintext `updatedAt` afterwards, exactly as for
  /// unencrypted tables.
  ///
  /// Rows whose content cannot be decrypted (missing key, unknown version,
  /// failed authentication) decode into *locked* placeholder rows and the
  /// verbatim ciphertext is preserved — never discarded: the local cache is
  /// plaintext-only and the row's updated_at watermark means a pull would
  /// never re-fetch it, so dropping the blob would lose the content forever.
  /// Missing keys alert at info severity, authentication failures at critical
  /// (possible tampering); storage handling is identical for both.
  Future<Syncable> _decodeIncoming(
    Type syncable,
    Map<String, dynamic> wireRow,
  ) async {
    final encryption = _encryption[syncable];
    if (encryption == null) return _fromJsons[syncable]!(wireRow);

    // Never mutate the caller's map (realtime payloads are not ours).
    final json = Map<String, dynamic>.from(wireRow);
    final contentEnc = json.remove(contentEncKey) as String?;
    final keyVersion = (json.remove(keyVersionKey) as num?)?.toInt();

    if (contentEnc == null) {
      // Legacy plaintext row, or an off-mode circle: pass through. This
      // mixed-state read path stays until the GA plaintext decommission.
      return _fromJsons[syncable]!(json);
    }

    final backendTable = _backendTables[syncable]!;
    final rowId = json[idKey] as String;
    final circleId = json[circleIdKey] as String?;

    // A row is "enforced-shaped" when its registered plaintext columns are all
    // nulled — the blob is then the only copy of the content. Rows that still
    // carry plaintext (shadow dual-writes, restored rows with a stale blob,
    // mid-transition rows) stay readable no matter what happens to the blob:
    // a readable row is never locked.
    final enforcedShaped = encryption.encryptedFields.every(
      (field) => json[field] == null,
    );

    if (circleId == null || keyVersion == null) {
      // Without a circle scope or a key version no key can be selected.
      // Likely a backend data bug rather than tampering: key-availability
      // severity, not critical.
      if (!enforcedShaped) return _fromJsons[syncable]!(json);
      _emitAlert(
        SyncEncryptionAlert(
          kind: SyncEncryptionAlertKind.keyUnavailable,
          severity: SyncEncryptionAlertSeverity.info,
          table: backendTable,
          rowId: rowId,
          circleId: circleId,
          keyVersion: keyVersion,
          message:
              'Encrypted row carries no '
              '${circleId == null ? 'circle_id' : 'key_version'} — cannot '
              'select a key; row stored locked',
        ),
      );
      return _lockRow(syncable, json, contentEnc, keyVersion);
    }

    final mode = await _fieldCipher!.modeFor(
      table: backendTable,
      circleId: circleId,
    );

    if (!enforcedShaped && mode != SyncEncryptionMode.enforced) {
      // Plaintext-authoritative row. In shadow mode the blob must still agree
      // with the plaintext — that parity check is what proves the pipeline
      // lossless before a circle is promoted.
      if (mode == SyncEncryptionMode.shadow) {
        await _verifyShadowParity(
          syncable,
          json,
          contentEnc: contentEnc,
          keyVersion: keyVersion,
          circleId: circleId,
        );
      }
      return _fromJsons[syncable]!(json);
    }

    // Blob-authoritative: enforced mode, or an enforced-shaped row pulled
    // while this client still resolves the circle as shadow/off (mode
    // transition skew — the row's shape wins, there is no plaintext to read).
    try {
      final fields = await _fieldCipher.decryptContent(
        table: backendTable,
        rowId: rowId,
        circleId: circleId,
        contentEnc: contentEnc,
        keyVersion: keyVersion,
      );
      // The blob can only ever contribute content: a crafted blob (someone
      // with the circle key) must not be able to spoof the plaintext envelope
      // the sync engine trusts (LWW timestamp, identity, tombstone, scope).
      fields.removeWhere((key, _) => _protectedWireKeys.contains(key));
      // Unknown fields from newer clients flow through (`fromJson` ignores
      // them) — blob evolution is the cipher's job, tolerance is ours.
      json.addAll(fields);
      return _fromJsons[syncable]!(json);
    } on SyncCipherMissingKeyException catch (e) {
      if (!enforcedShaped) return _fromJsons[syncable]!(json);
      _emitAlert(
        SyncEncryptionAlert(
          kind: SyncEncryptionAlertKind.keyUnavailable,
          severity: SyncEncryptionAlertSeverity.info,
          table: backendTable,
          rowId: rowId,
          circleId: circleId,
          keyVersion: keyVersion,
          message: 'Row stored locked until its key arrives: $e',
        ),
      );
      return _lockRow(syncable, json, contentEnc, keyVersion);
    } on SyncCipherAuthException catch (e) {
      _emitAlert(
        SyncEncryptionAlert(
          kind: SyncEncryptionAlertKind.possibleTampering,
          severity: SyncEncryptionAlertSeverity.critical,
          table: backendTable,
          rowId: rowId,
          circleId: circleId,
          keyVersion: keyVersion,
          message: 'Blob failed authentication (tampered or transplanted): $e',
        ),
      );
      if (!enforcedShaped) return _fromJsons[syncable]!(json);
      return _lockRow(syncable, json, contentEnc, keyVersion);
    }
  }

  /// Decodes an undecryptable row into a locked placeholder model and stashes
  /// the verbatim ciphertext for the write path (and later local retry).
  Syncable _lockRow(
    Type syncable,
    Map<String, dynamic> json,
    String contentEnc,
    int? keyVersion,
  ) {
    final encryption = _encryption[syncable]!;
    for (final field in encryption.encryptedFields) {
      json[field] = encryption.lockedFieldPlaceholders[field];
    }
    final item = _fromJsons[syncable]!(json);
    // Newest-wins, mirroring the incoming-queue collapse: locked decodes can
    // complete out of order, and the preserved ciphertext must stay aligned
    // with the row version that ultimately gets written — a version-mismatched
    // instruction would be dropped at write time, stripping the locked row of
    // its blob.
    final pending = _pendingLockedBlobs[syncable]!;
    final existing = pending[item.id];
    if (existing == null || !existing.forUpdatedAt.isAfter(item.updatedAt)) {
      pending[item.id] = _PendingLockedBlob(
        contentEnc: contentEnc,
        keyVersion: keyVersion,
        forUpdatedAt: item.updatedAt,
      );
    }
    return item;
  }

  /// Shadow-mode parity check: the decrypted blob must agree with the
  /// authoritative plaintext columns. Mismatches are reported (field names
  /// only — never values); a missing key just skips verification (key
  /// telemetry is not the pull path's job), a failed authentication is
  /// reported as possible tampering.
  Future<void> _verifyShadowParity(
    Type syncable,
    Map<String, dynamic> json, {
    required String contentEnc,
    required int keyVersion,
    required String circleId,
  }) async {
    final encryption = _encryption[syncable]!;
    final backendTable = _backendTables[syncable]!;
    final rowId = json[idKey] as String;
    try {
      final fields = await _fieldCipher!.decryptContent(
        table: backendTable,
        rowId: rowId,
        circleId: circleId,
        contentEnc: contentEnc,
        keyVersion: keyVersion,
      );
      final mismatched = [
        for (final field in encryption.encryptedFields)
          if (!_parityEquals(json[field], fields[field])) field,
      ];
      if (mismatched.isNotEmpty) {
        _emitAlert(
          SyncEncryptionAlert(
            kind: SyncEncryptionAlertKind.shadowParityMismatch,
            severity: SyncEncryptionAlertSeverity.warning,
            table: backendTable,
            rowId: rowId,
            circleId: circleId,
            keyVersion: keyVersion,
            message:
                'Decrypted blob disagrees with the plaintext column(s) '
                '$mismatched',
          ),
        );
      }
    } on SyncCipherMissingKeyException {
      _logger.fine(
        'No key to verify shadow parity for $rowId in $backendTable',
      );
    } on SyncCipherAuthException catch (e) {
      _emitAlert(
        SyncEncryptionAlert(
          kind: SyncEncryptionAlertKind.possibleTampering,
          severity: SyncEncryptionAlertSeverity.critical,
          table: backendTable,
          rowId: rowId,
          circleId: circleId,
          keyVersion: keyVersion,
          message:
              'Shadow blob failed authentication (tampered or '
              'transplanted): $e',
        ),
      );
    }
  }

  /// Value equality for the shadow parity check. Timestamps may round-trip
  /// with different serializations (Postgres `+00:00` vs Dart `Z`), so two
  /// parseable strings compare as instants.
  static bool _parityEquals(dynamic plaintext, dynamic decrypted) {
    if (const DeepCollectionEquality().equals(plaintext, decrypted)) {
      return true;
    }
    if (plaintext is String && decrypted is String) {
      final p = DateTime.tryParse(plaintext);
      final d = DateTime.tryParse(decrypted);
      if (p != null && d != null) return p.isAtSameMomentAs(d);
    }
    return false;
  }

  /// Drops a pending locked-blob instruction when [item]'s write is skipped,
  /// so a stale instruction can never be applied to a different version's
  /// write later. Versioned on `updatedAt`, like the quarantines.
  void _discardPendingLockedBlob(Type syncable, Syncable item) {
    final pending = _pendingLockedBlobs[syncable];
    if (pending == null) return;
    final entry = pending[item.id];
    if (entry != null && entry.forUpdatedAt == item.updatedAt) {
      pending.remove(item.id);
    }
  }

  /// PostgREST surfaces transport-level failures — rate limits (429) and server
  /// errors (5xx) — as [PostgrestException]s with the HTTP status in [code], the
  /// same field that otherwise carries a Postgres SQLSTATE (e.g. '23505',
  /// '42501'). Those SQLSTATEs are PERMANENT for the row (constraint / RLS /
  /// check) and must be quarantined; 429 / 5xx are TRANSIENT and must be retried.
  ///
  /// We parse [code] as an int and treat it as transient only for a real HTTP
  /// status (429, or 5xx below 600). The `< 600` guard stops a 5-digit SQLSTATE
  /// like 23505 from being misread as ">= 500".
  static bool _isTransientPostgrest(PostgrestException e) {
    final status = int.tryParse(e.code ?? '');
    return status != null && (status == 429 || (status >= 500 && status < 600));
  }

  Future<void> _processOutgoing(Type syncable) async {
    final outQueue = _outQueues[syncable]!;
    final backendTable = _backendTables[syncable]!;
    final quarantined = _outgoingQuarantined[syncable]!;

    while (_syncingEnabled && outQueue.isNotEmpty) {
      // GAM-389: push every queued row regardless of owner; RLS authorizes the
      // write and onConflict:id makes it an idempotent upsert.
      //
      // Skip rows the backend has permanently rejected (quarantined): a single
      // poison row must never re-wedge the whole table (MC-424 §B). A row whose
      // version moved on since it was quarantined (a local edit that may fix the
      // rejection) is let through to retry. The same versioned skip applies to
      // rows deferred because their circle key is unavailable.
      final outgoing = outQueue.values
          .where((s) {
            final failedAt = quarantined[s.id];
            return failedAt == null || s.updatedAt.isAfter(failedAt);
          })
          .where((s) => !_isEncryptionDeferred(syncable, s))
          .toSet();
      outQueue.clear();

      if (outgoing.isEmpty) continue;

      assert(!outgoing.any((s) => s.userId?.isEmpty ?? true));

      // Encode rows for the wire (the field-encryption seam) concurrently:
      // fold registered content fields into the blob according to the
      // circle's mode. Rows that cannot be encrypted yet (missing circle key
      // in enforced mode) are deferred — withheld from the wire, never
      // dropped.
      final encoded = <Syncable, Map<String, dynamic>>{};
      final encodeResults = await Future.wait(
        outgoing.map(
          (row) async =>
              (row: row, payload: await _encodeOutgoing(syncable, row)),
        ),
      );
      for (final result in encodeResults) {
        final payload = result.payload;
        if (payload != null) encoded[result.row] = payload;
      }

      if (encoded.isEmpty) continue;

      _logger.info(
        'Syncing ${encoded.length} items to backend table $backendTable',
      );

      try {
        await _upsertPayloads(backendTable, encoded.values);
        await _markPushed(syncable, encoded.keys.toSet());
      } on PostgrestException catch (batchError) {
        if (_isTransientPostgrest(batchError)) {
          // PostgREST reports rate limits (429) and server errors (5xx) as
          // PostgrestExceptions too. Those are transient: re-enqueue the WHOLE
          // batch and back off — never fall through to per-row, which would
          // quarantine healthy rows over a passing backend hiccup (MC-424 §B/§C).
          for (final row in encoded.keys) {
            outQueue[row.id] = row;
          }
          _logger.warning(
            'Transient PostgrestException pushing to $backendTable '
            '($batchError); will retry',
          );
          return;
        }

        // A permanent rejection of at least one row in the batch (constraint /
        // RLS / check). Previously this threw out of the whole table push and
        // wedged every other row — and every later table — until the poison row
        // was gone. Instead, retry row-by-row so the good rows still flush and
        // the poison row is isolated (MC-424 §B).
        _logger.warning(
          'Batch upsert to $backendTable rejected ($batchError); '
          'falling back to per-row',
        );

        final succeeded = <Syncable>{};
        final retry = <Syncable>{};
        for (final row in encoded.keys) {
          try {
            await _upsertPayloads(backendTable, [encoded[row]!]);
            succeeded.add(row);
          } on PostgrestException catch (rowError, rowStack) {
            if (_isTransientPostgrest(rowError)) {
              // Transient (rate limit / server error) mid-fallback — keep for
              // retry; do NOT quarantine over a passing backend failure.
              retry.add(row);
            } else {
              // Permanent rejection of this specific row. Quarantine at this
              // version + report; the row stays in the local db with dirty=true,
              // so NOTHING is lost — it stops jamming the queue, and a later edit
              // (newer updatedAt) or a restart gives it another chance.
              quarantined[row.id] = row.updatedAt;
              _logger.severe(
                'Quarantined poison row ${row.id} in $backendTable after '
                'backend rejection: $rowError\n$rowStack',
              );
            }
          } catch (_) {
            // Transient (e.g. network dropped mid-fallback) — keep for retry.
            retry.add(row);
          }
        }

        await _markPushed(syncable, succeeded);

        if (retry.isNotEmpty) {
          // Re-enqueue the rows we never got a verdict on so the next loop pass
          // retries them, then back off (don't spin this pass).
          for (final row in retry) {
            outQueue[row.id] = row;
          }
          return;
        }
      } catch (batchError) {
        // Transient failure (network, etc.): re-enqueue the whole batch so it is
        // retried next pass — never dropped (MC-424 §C) — and back off.
        for (final row in encoded.keys) {
          outQueue[row.id] = row;
        }
        _logger.warning(
          'Transient failure pushing to $backendTable ($batchError); '
          'will retry',
        );
        return;
      }
    }
  }

  Future<void> _upsertPayloads(
    String backendTable,
    Iterable<Map<String, dynamic>> payloads,
  ) async {
    await _supabaseClient
        .from(backendTable)
        .upsert(
          payloads.toList(),
          // GAM-389: conflict on id alone — one row per entity, not per user.
          onConflict: idKey,
        );
  }

  /// Builds the wire payload for [row] — the push half of the encryption
  /// seam. Returns `null` when the row must be withheld (enforced mode, key
  /// unavailable); the row is then deferred, never dropped.
  Future<Map<String, dynamic>?> _encodeOutgoing(
    Type syncable,
    Syncable row,
  ) async {
    final json = row.toJson();
    final encryption = _encryption[syncable];
    if (encryption == null) return json;

    final backendTable = _backendTables[syncable]!;

    // Locked guard, mode-independent: a locked row's content fields are
    // placeholders and must NEVER be encrypted or pushed as plaintext. A
    // legitimate plaintext-column write (e.g. a `deleted` tombstone) goes out
    // with the ORIGINAL ciphertext forwarded verbatim — the seam never
    // synthesizes a blob from a row it could not decrypt.
    if (row is EncryptedSyncable && row.locked) {
      for (final field in encryption.encryptedFields) {
        json[field] = null;
      }
      json[contentEncKey] = row.lockedContentEnc;
      json[keyVersionKey] = row.lockedKeyVersion;
      if (row.lockedContentEnc == null) {
        _logger.warning(
          'Locked row ${row.id} in $backendTable has no preserved ciphertext; '
          'pushing plaintext envelope only',
        );
      }
      return json;
    }

    final circleId = json[circleIdKey] as String?;
    final mode = circleId == null
        ? SyncEncryptionMode.off
        : await _fieldCipher!.modeFor(table: backendTable, circleId: circleId);

    try {
      switch (mode) {
        case SyncEncryptionMode.off:
          // Plaintext pass-through, but explicitly null the blob columns so a
          // circle dropping back from shadow/enforced sheds its stale blobs
          // (decrypt-and-restore). Registration implies the backend columns
          // exist (the schema migration is a registration prerequisite).
          json[contentEncKey] = null;
          json[keyVersionKey] = null;
        case SyncEncryptionMode.shadow:
          // Dual-write: plaintext stays authoritative AND the blob rides
          // along so parity can be verified on pull. A missing key only
          // degrades this row to plaintext-only — shadow's contract is that
          // plaintext is complete; key-distribution telemetry is not the push
          // path's job.
          try {
            final blob = await _encryptFields(syncable, json, circleId!);
            json[contentEncKey] = blob.contentEnc;
            json[keyVersionKey] = blob.keyVersion;
          } on SyncCipherMissingKeyException catch (e) {
            json[contentEncKey] = null;
            json[keyVersionKey] = null;
            _logger.warning(
              'No key to shadow-encrypt ${row.id} in $backendTable ($e); '
              'pushed plaintext only',
            );
          }
        case SyncEncryptionMode.enforced:
          final blob = await _encryptFields(syncable, json, circleId!);
          for (final field in encryption.encryptedFields) {
            json[field] = null;
          }
          json[contentEncKey] = blob.contentEnc;
          json[keyVersionKey] = blob.keyVersion;
      }
    } on SyncCipherMissingKeyException catch (e) {
      // Enforced mode without the circle key: the row must not leave the
      // device in plaintext. Defer it (it stays dirty locally) and surface a
      // key-availability alert — once per row version, not per loop pass.
      _deferForEncryption(syncable, row, circleId, e);
      return null;
    }

    return json;
  }

  Future<SyncEncryptedBlob> _encryptFields(
    Type syncable,
    Map<String, dynamic> json,
    String circleId,
  ) {
    final encryption = _encryption[syncable]!;
    return _fieldCipher!.encryptContent(
      table: _backendTables[syncable]!,
      rowId: json[idKey] as String,
      circleId: circleId,
      fields: {
        for (final field in encryption.encryptedFields) field: json[field],
      },
    );
  }

  bool _isEncryptionDeferred(Type syncable, Syncable row) {
    final deferred = _encryptionDeferred[syncable]?[row.id];
    // A strictly-newer local edit is let through to retry (the key situation
    // or the row itself may have changed), mirroring the quarantine rules.
    return deferred != null && !row.updatedAt.isAfter(deferred.updatedAt);
  }

  void _deferForEncryption(
    Type syncable,
    Syncable row,
    String? circleId,
    SyncCipherMissingKeyException cause,
  ) {
    final deferred = _encryptionDeferred[syncable]!;
    final previous = deferred[row.id];
    deferred[row.id] = row;
    final alreadyAlerted =
        previous != null && !row.updatedAt.isAfter(previous.updatedAt);
    if (alreadyAlerted) return;
    _emitAlert(
      SyncEncryptionAlert(
        kind: SyncEncryptionAlertKind.keyUnavailable,
        severity: SyncEncryptionAlertSeverity.info,
        table: _backendTables[syncable]!,
        rowId: row.id,
        circleId: circleId,
        keyVersion: null,
        message: 'Push deferred until a circle key is available: $cause',
      ),
    );
  }

  void _emitAlert(SyncEncryptionAlert alert) {
    _logger.warning(alert.toString());
    try {
      _onEncryptionAlert?.call(alert);
    } catch (e, s) {
      // coverage:ignore-start
      _logger.severe('onEncryptionAlert callback threw: $e\n$s');
      // coverage:ignore-end
    }
  }

  /// Marks [pushed] rows as synced: records them as sent, clears their `dirty`
  /// flag, and advances counters / the last-pushed watermark.
  Future<void> _markPushed(Type syncable, Set<Syncable> pushed) async {
    if (pushed.isEmpty) return;

    final table = _localTables[syncable]!;
    _sentItems[syncable]!.addAll(pushed);

    // The pushed rows are now in sync with the backend — clear their dirty flag
    // so they are not re-pushed next cycle / after a restart. Guard each clear on
    // the exact updatedAt we pushed: if the user edited the row again while this
    // batch was in flight, its updatedAt has moved on, the match fails, dirty
    // stays true, and the newer edit pushes next cycle.
    final cleanCompanion =
        _companions[syncable]!(dirty: const Value(false))
            as UpdateCompanion<Syncable>;
    await _localDb.batch((batch) {
      for (final row in pushed) {
        batch.update(
          table,
          cleanCompanion,
          where: (tbl) =>
              tbl.id.equals(row.id) & tbl.updatedAt.equals(row.updatedAt),
        );
      }
    });

    _nSyncedToBackend[syncable] = nSyncedToBackend(syncable) + pushed.length;

    final lastUpdatedAtForThisBatch = pushed.map((r) => r.updatedAt).max;
    if (_lastPushedTimestamp(syncable) == null ||
        lastUpdatedAtForThisBatch.isAfter(_lastPushedTimestamp(syncable)!)) {
      await _updateLastPushedTimestamp(syncable, lastUpdatedAtForThisBatch);
    }
  }

  Future<void> _processIncoming(Type syncable) async {
    final inQueue = _inQueues[syncable]!;

    if (inQueue.isEmpty) return;

    final sentItems = _sentItems[syncable]!;
    final receivedItems = _receivedItems[syncable]!;
    final quarantined = _incomingQuarantined[syncable]!;

    final itemsToWrite = <String, Syncable>{};

    for (final item in inQueue) {
      // Skip if already processed, or if a previous write of this row was
      // permanently rejected locally (e.g. it collides with a divergent local
      // row) — quarantined so it can't re-wedge the whole incoming batch every
      // pull (MC-424 §B). This is the INCOMING quarantine only; a row we failed
      // to push is deliberately still pullable. The quarantine is versioned: a
      // strictly-newer backend version (a remote fix / tombstone) is let through
      // to retry rather than dropped until restart.
      final failedAt = quarantined[item.id];
      if (sentItems.contains(item) ||
          receivedItems.contains(item) ||
          (failedAt != null && !item.updatedAt.isAfter(failedAt))) {
        // A skipped item must also drop its locked-blob instruction (if this
        // exact version produced one), or it could mis-apply to a later write.
        _discardPendingLockedBlob(syncable, item);
        continue;
      }
      // Newest-wins collapse per row id: queue arrival order is not version
      // order — encrypted rows decode asynchronously and can complete out of
      // order, and a websocket may redeliver across reconnects — so keeping
      // the last-enqueued entry could let an older version shadow a newer one
      // until the next reconcile.
      final queued = itemsToWrite[item.id];
      if (queued == null || item.updatedAt.isAfter(queued.updatedAt)) {
        itemsToWrite[item.id] = item;
      }
    }

    inQueue.clear();

    await _batchWriteIncoming(syncable, itemsToWrite);

    receivedItems.addAll(itemsToWrite.values);
    _nSyncedFromBackend[syncable] =
        nSyncedFromBackend(syncable) + itemsToWrite.length;
  }

  Future<void> _batchWriteIncoming<S extends Syncable>(
    Type syncable,
    Map<String, S> incomingItems,
  ) async {
    if (incomingItems.isEmpty) return;

    final table = _localTables[syncable]! as TableInfo<SyncableTable, S>;

    final existingItems =
        await (_localDb.select(
          table,
        )..where((tbl) => tbl.id.isIn(incomingItems.keys))).get().then(
          (items) =>
              Map.fromEntries(items.map((i) => MapEntry(i.id, i.updatedAt))),
        );

    // Decide insert-vs-replace per row, keeping each row's verdict so a failed
    // batch can be retried one row at a time.
    final decided = <_IncomingWrite>[];
    for (final incomingItem in incomingItems.values) {
      final existingUpdatedAt = existingItems[incomingItem.id];
      if (existingUpdatedAt == null) {
        decided.add((
          id: incomingItem.id,
          updatedAt: incomingItem.updatedAt,
          companion: incomingItem.toCompanion(),
          insert: true,
        ));
      } else if (incomingItem.updatedAt.isAfter(existingUpdatedAt)) {
        decided.add((
          id: incomingItem.id,
          updatedAt: incomingItem.updatedAt,
          companion: incomingItem.toCompanion(),
          insert: false,
        ));
      } else {
        // Local copy is newer — leave it (and its dirty flag) untouched so a
        // pending local edit still gets pushed. The discarded version's
        // locked-blob instruction (if any) goes with it.
        _discardPendingLockedBlob(syncable, incomingItem);
      }
    }

    if (decided.isEmpty) return;

    try {
      await _writeIncomingRows(syncable, table, decided);
    } on Exception catch (batchError) {
      // A poison row (e.g. one that collides with a divergent local row on a
      // secondary UNIQUE constraint) used to roll back the whole incoming
      // transaction and wedge every pull for the table. Retry row-by-row so the
      // good rows still land and the poison row is isolated (MC-424 §B).
      _logger.warning(
        'Incoming batch write to ${_backendTables[syncable]} failed '
        '($batchError); falling back to per-row',
      );
      final quarantined = _incomingQuarantined[syncable]!;
      for (final write in decided) {
        try {
          await _writeIncomingRows(syncable, table, [write]);
        } catch (rowError, rowStack) {
          // Quarantine at this row's version, so a strictly-newer backend
          // version later supersedes it instead of being dropped until restart.
          quarantined[write.id] = write.updatedAt;
          _logger.severe(
            'Quarantined poison incoming row ${write.id} in '
            '${_backendTables[syncable]}: $rowError\n$rowStack',
          );
        }
      }
    }
  }

  /// Writes a set of already-decided incoming [writes] (insert or replace) and
  /// clears their `dirty` flag, in ONE transaction.
  ///
  /// A pulled row is not a local change. `toCompanion()` defaults dirty=true (so
  /// the app's own writes are pushed), so the incoming write must clear it —
  /// otherwise the next sync would push the row straight back to the backend.
  /// The write + clear share a transaction so the local-change stream only ever
  /// observes the final state (dirty=false); otherwise the intermediate
  /// dirty=true write could be picked up by the local-changes subscription and
  /// queued for push before we clear it.
  Future<void> _writeIncomingRows<S extends Syncable>(
    Type syncable,
    TableInfo<SyncableTable, S> table,
    List<_IncomingWrite> writes,
  ) async {
    if (writes.isEmpty) return;

    // Type the lists as Insertable<S> (the concrete syncable type) — the records
    // carry the companion as the erased UpdateCompanion<Syncable>, but batch
    // insertAll/replaceAll expect Insertable<S>. The runtime object is the
    // concrete companion (e.g. CircleMembersCompanion implements Insertable<S>),
    // so the cast is sound and keeps the call statically type-safe.
    final itemsToInsert = <Insertable<S>>[
      for (final w in writes)
        if (w.insert) w.companion as Insertable<S>,
    ];
    final itemsToReplace = <Insertable<S>>[
      for (final w in writes)
        if (!w.insert) w.companion as Insertable<S>,
    ];
    final writtenIds = [for (final w in writes) w.id];

    await _localDb.transaction(() async {
      await _localDb.batch((batch) {
        batch.insertAll(table, itemsToInsert);
        batch.replaceAll(table, itemsToReplace);
      });

      final cleanCompanion =
          _companions[syncable]!(dirty: const Value(false))
              as UpdateCompanion<S>;
      // Chunk the id list: a single `isIn` over a large initial/backlog sync can
      // exceed SQLite's variable limit (999) → "too many SQL variables".
      for (final idChunk in writtenIds.slices(500)) {
        await (_localDb.update(
          table,
        )..where((tbl) => tbl.id.isIn(idChunk))).write(cleanCompanion);
      }

      await _applyPendingLockedBlobs<S>(syncable, table, writes);
    });
  }

  /// Applies the locked-blob instructions produced at decode time to the rows
  /// just written: sets the `locked` flag and preserves the verbatim
  /// ciphertext in the fallback columns, atomically with the row write.
  /// (The reverse — unlocking — needs no instruction: a decrypted model's
  /// `toCompanion` carries `locked: false` and null fallbacks.)
  Future<void> _applyPendingLockedBlobs<S extends Syncable>(
    Type syncable,
    TableInfo<SyncableTable, S> table,
    List<_IncomingWrite> writes,
  ) async {
    final pending = _pendingLockedBlobs[syncable];
    if (pending == null || pending.isEmpty) return;

    for (final write in writes) {
      final entry = pending[write.id];
      if (entry == null) continue;
      if (entry.forUpdatedAt != write.updatedAt) {
        // The instruction belongs to a different version of this row than the
        // one being written. Keep it only if it is newer (its own write may
        // still be queued behind this one); a stale one is dropped.
        if (!entry.forUpdatedAt.isAfter(write.updatedAt)) {
          pending.remove(write.id);
        }
        continue;
      }
      pending.remove(write.id);
      final lockCompanion =
          (_companions[syncable]! as EncryptedCompanionConstructor)(
                locked: const Value(true),
                lockedContentEnc: Value(entry.contentEnc),
                lockedKeyVersion: Value(entry.keyVersion),
              )
              as UpdateCompanion<S>;
      await (_localDb.update(
        table,
      )..where((tbl) => tbl.id.equals(write.id))).write(lockCompanion);
    }
  }

  DateTime? _lastPushedTimestamp(Type syncable) {
    return _syncTimestampStorage?.getSyncTimestamp(
      _keyForPersistentStorage(TimestampType.lastSyncToBackend, syncable),
    );
  }

  Future<void> _updateLastPushedTimestamp(
    Type syncable,
    DateTime timestamp,
  ) async {
    await _syncTimestampStorage?.setSyncTimestamp(
      _keyForPersistentStorage(TimestampType.lastSyncToBackend, syncable),
      timestamp,
    );
  }

  DateTime? _lastPulledTimestamp(Type syncable) {
    return _syncTimestampStorage?.getSyncTimestamp(
      _keyForPersistentStorage(TimestampType.lastSyncFromBackend, syncable),
    );
  }

  Future<void> _updateLastPulledTimeStamp(
    Type syncable,
    DateTime timestamp,
  ) async {
    await _syncTimestampStorage?.setSyncTimestamp(
      _keyForPersistentStorage(TimestampType.lastSyncFromBackend, syncable),
      timestamp,
    );
  }

  void _clearLocalSubscriptions() {
    for (final subscription in _localSubscriptions.values) {
      subscription.cancel();
    }
    _localSubscriptions.clear();
  }

  String _keyForPersistentStorage(TimestampType type, Type syncable) {
    return 'syncable_${userId}_${type.name}_${_localTables[syncable]!.actualTableName}';
  }

  bool _otherDevicesActive() {
    // Assume other devices are active if we don't have a timestamp for the last
    // time they were active.
    if (lastTimeOtherDeviceWasActive == null) return true;
    return DateTime.now().difference(lastTimeOtherDeviceWasActive!) <
        _devicesConsideredInactiveAfter;
  }
}

typedef CompanionConstructor =
    Object Function({
      Value<int> rowid,
      Value<String> id,
      Value<String?> userId,
      Value<DateTime> updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
    });

/// The companion constructor of an [EncryptedSyncableTable] — the base
/// columns plus the locked-row fallbacks. A generated Drift companion
/// constructor for such a table satisfies this automatically; registration
/// verifies it.
typedef EncryptedCompanionConstructor =
    Object Function({
      Value<int> rowid,
      Value<String> id,
      Value<String?> userId,
      Value<DateTime> updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
      Value<bool> locked,
      Value<String?> lockedContentEnc,
      Value<int?> lockedKeyVersion,
    });

/// Opts a syncable into the field-encryption seam
/// ([SyncManager.registerSyncable]'s `encryption` parameter).
///
/// Requirements:
/// * The model implements [EncryptedSyncable] and its table implements
///   [EncryptedSyncableTable] (local locked-row fallback columns).
/// * The model's `toJson` uses the wire (snake_case) names listed in
///   [encryptedFields], and tolerates unknown JSON keys in `fromJson`.
/// * The backend table has `content_enc` (text) and `key_version` (int)
///   columns, and the registered content columns are nullable. **This schema
///   migration is a hard prerequisite for registering** — even off-mode
///   pushes attach explicit `content_enc`/`key_version` nulls (that is what
///   lets a circle shed stale blobs after decrypt-and-restore).
/// * Rows are circle-scoped via a plaintext `circle_id` column; rows with a
///   null `circle_id` sync as plaintext (there is no key scope to encrypt
///   under).
class SyncEncryption {
  SyncEncryption({
    required this.encryptedFields,
    required this.lockedFieldPlaceholders,
  });

  /// Wire JSON keys folded into the encrypted blob on push and merged back on
  /// pull. Anything sync, RLS, or the backend must read — `id`, `user_id`,
  /// `updated_at`, `deleted`, `circle_id`, foreign-key columns — must NOT be
  /// listed (registration rejects the seam-critical ones).
  final Set<String> encryptedFields;

  /// Values substituted for [encryptedFields] when a row arrives
  /// undecryptable, so the locked placeholder row can still be constructed
  /// via `fromJson`. Must cover every encrypted field `fromJson` requires;
  /// fields without a placeholder decode as `null`.
  final Map<String, dynamic> lockedFieldPlaceholders;
}

/// A decode-time instruction to persist a row's undecryptable ciphertext into
/// its locked-row fallback columns, pinned to the row version it was produced
/// for.
class _PendingLockedBlob {
  const _PendingLockedBlob({
    required this.contentEnc,
    required this.keyVersion,
    required this.forUpdatedAt,
  });

  final String contentEnc;
  final int? keyVersion;
  final DateTime forUpdatedAt;
}

/// An incoming row write whose insert-vs-replace verdict has been decided,
/// pinned to the row version it carries.
typedef _IncomingWrite = ({
  String id,
  DateTime updatedAt,
  UpdateCompanion<Syncable> companion,
  bool insert,
});

enum TimestampType {
  lastSyncFromBackend('lastSyncFromBackend'),
  lastSyncToBackend('lastSyncToBackend');

  const TimestampType(this.name);
  final String name;
}
