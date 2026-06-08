import 'dart:async';

import 'package:collection/collection.dart';
import 'package:drift/drift.dart';
import 'package:logging/logging.dart';
import 'package:supabase/supabase.dart';
import 'package:syncable/src/supabase_names.dart';
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
  SyncManager({
    required T localDatabase,
    required SupabaseClient supabaseClient,
    Duration syncInterval = const Duration(seconds: 1),
    int maxRows = 1000,
    SyncTimestampStorage? syncTimestampStorage,
    Duration otherDevicesConsideredInactiveAfter = const Duration(minutes: 2),
    Duration reconcileOverlap = const Duration(seconds: 10),
  }) : _localDb = localDatabase,
       _supabaseClient = supabaseClient,
       _syncInterval = syncInterval,
       _maxRows = maxRows,
       _syncTimestampStorage = syncTimestampStorage,
       _devicesConsideredInactiveAfter = otherDevicesConsideredInactiveAfter,
       _reconcileOverlap = reconcileOverlap,
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
  final Map<Type, Set<String>> _outgoingQuarantined = {};
  final Map<Type, Set<String>> _incomingQuarantined = {};

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
  /// The generic type parameter must be provided and must be a  concrete
  /// subclass of [Syncable].
  void registerSyncable<S extends Syncable>({
    required String backendTable,
    required Syncable Function(Map<String, dynamic>) fromJson,
    required CompanionConstructor companionConstructor,
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

    _syncables.add(S);
    _localTables[S] = _localDb.getTable<S>();
    _backendTables[S] = backendTable;
    _fromJsons[S] = fromJson;
    _companions[S] = companionConstructor;
    _inQueues[S] = {};
    _outQueues[S] = {};
    _sentItems[S] = {};
    _receivedItems[S] = {};
    _outgoingQuarantined[S] = {};
    _incomingQuarantined[S] = {};
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

  Future _onDependenciesChanged(String reason) async {
    _maybeSubscribeToLocalChanges();
    _maybeSubscribeToBackendChanges();

    await _syncTables(reason);
  }

  void _maybeSubscribeToLocalChanges() {
    _clearLocalSubscriptions();

    if (!_syncingEnabled) {
      _logger.warning(
        'Not subscribed to local changes because syncing is disabled',
      );
      return;
    }

    if (userId.isEmpty) {
      _logger.warning(
        'Not subscribed to local changes because user ID is empty',
      );
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

  void _pushLocalChangesToOutQueue(Type syncable, Iterable<Syncable> rows) {
    final outQueue = _outQueues[syncable]!;
    final receivedItems = _receivedItems[syncable]!;

    bool updateHasNotBeenSentYet(Syncable row) =>
        row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0)) &&
        row.updatedAt.isAfter(_lastPushedTimestamp(syncable) ?? DateTime(0));

    var enqueuedAny = false;
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
      enqueuedAny = true;
    }

    // Drain the new work now rather than on the next backstop tick. Reached from
    // the local-change Drift subscription (writer side) and from _syncTable.
    if (enqueuedAny) _wake();
  }

  void _maybeSubscribeToBackendChanges() {
    final otherDevicesActive = _otherDevicesActive();

    if (!_syncingEnabled || !otherDevicesActive) {
      if (_backendSubscription != null) {
        _backendSubscription?.unsubscribe();
        _backendSubscription = null;
      }

      String reason;

      if (!__syncingEnabled) {
        reason = 'syncing is disabled';
      } else if (userId.isEmpty) {
        reason = 'the user ID is empty';
      } else if (!otherDevicesActive) {
        reason = 'no other devices are active';
      } else {
        reason = '... good question. Please file an issue';
      }

      _logger.warning('Not subscribed to backend changes because $reason');

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
            final item = _fromJsons[syncable]!(p.newRecord);
            _inQueues[syncable]!.add(item);
            // Drain the realtime delivery now rather than on the next backstop
            // tick — this is the reader-side half of the sub-second path.
            _wake();
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
  Future<void> syncTables() async {
    await _syncTables('Manual sync');
  }

  Future<void> _syncTables(String reason) async {
    if (!__syncingEnabled) {
      _logger.warning('Tables not getting synced because syncing is disabled');
      return;
    }

    if (userId.isEmpty) {
      _logger.warning('Tables not getting synced because user ID is empty');
      return;
    }

    _logger.info('Syncing all tables. Reason: $reason');

    for (final syncable in _syncables) {
      await _syncTable(syncable);
    }

    _nFullSyncs++;
  }

  Future<void> _syncTable(Type syncable) async {
    if (!_syncingEnabled) return;

    final localItems = await _localDb.select(_localTables[syncable]!).get();

    // Check after async gap
    if (!_syncingEnabled) return;

    assert(_userId.isNotEmpty);
    // GAM-389: push ALL local rows, not just this user's. The updatedAt /
    // receivedItems dedup below still guards against echoing pulled rows.
    _pushLocalChangesToOutQueue(syncable, localItems);

    if (_skipSyncFromBackend(syncable)) {
      _logger.info(
        'Skipping sync of table ${_backendTables[syncable]} from backend '
        'because no other device was active since last sync',
      );
      return;
    }

    final localItemsUpdatedAt = {for (final i in localItems) i.id: i.updatedAt};

    assert(_userId.isNotEmpty);
    // Capture the watermark BEFORE fetching: any row written while this pull is
    // in flight has updatedAt >= pullStartedAt, so the next reconcile's
    // `> pullStartedAt - overlap` filter still catches it.
    final pullStartedAt = DateTime.now().toUtc();
    final backendItems = await _fetchBackendItemMetadata(syncable);

    final itemsToPull = _getItemsToPullFromBackend(
      backendItems,
      localItemsUpdatedAt,
    );

    if (itemsToPull.isNotEmpty) {
      _logger.info(
        "Syncing ${itemsToPull.length} items from backend table '${_backendTables[syncable]}'",
      );
    }

    // Use batches because all the UUIDs make the URI become too long otherwise.
    for (final batch in itemsToPull.slices(100)) {
      if (!_syncingEnabled) return;
      final pulledBatch = await _supabaseClient
          .from(_backendTables[syncable]!)
          .select()
          // GAM-389: no user_id filter — pull whatever RLS permits.
          .inFilter(idKey, batch)
          .then((data) => data.map(_fromJsons[syncable]!));

      _inQueues[syncable]!.addAll(pulledBatch);
      // A reconcile (off-loop) found rows to pull — wake the loop to write them.
      _wake();
    }

    await _updateLastPulledTimeStamp(syncable, pullStartedAt);
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
    Type syncable,
  ) async {
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
    final lastPulled = _lastPulledTimestamp(syncable)?.toUtc();
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
      // poison row must never re-wedge the whole table (MC-424 §B).
      final outgoing = outQueue.values
          .where((s) => !quarantined.contains(s.id))
          .toSet();
      outQueue.clear();

      if (outgoing.isEmpty) continue;

      _logger.info(
        'Syncing ${outgoing.length} items to backend table $backendTable',
      );

      assert(!outgoing.any((s) => s.userId?.isEmpty ?? true));

      try {
        await _upsertRows(backendTable, outgoing);
        await _markPushed(syncable, outgoing);
      } on PostgrestException catch (batchError) {
        if (_isTransientPostgrest(batchError)) {
          // PostgREST reports rate limits (429) and server errors (5xx) as
          // PostgrestExceptions too. Those are transient: re-enqueue the WHOLE
          // batch and back off — never fall through to per-row, which would
          // quarantine healthy rows over a passing backend hiccup (MC-424 §B/§C).
          for (final row in outgoing) {
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
        for (final row in outgoing) {
          try {
            await _upsertRows(backendTable, {row});
            succeeded.add(row);
          } on PostgrestException catch (rowError, rowStack) {
            if (_isTransientPostgrest(rowError)) {
              // Transient (rate limit / server error) mid-fallback — keep for
              // retry; do NOT quarantine over a passing backend failure.
              retry.add(row);
            } else {
              // Permanent rejection of this specific row. Quarantine + report;
              // the row stays in the local db with dirty=true, so NOTHING is
              // lost — it simply stops jamming the queue and retries on next
              // restart.
              quarantined.add(row.id);
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
        for (final row in outgoing) {
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

  Future<void> _upsertRows(String backendTable, Iterable<Syncable> rows) async {
    await _supabaseClient.from(backendTable).upsert(
      rows.map((x) => x.toJson()).toList(),
      // GAM-389: conflict on id alone — one row per entity, not per user.
      onConflict: idKey,
    );
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
      // to push is deliberately still pullable.
      if (sentItems.contains(item) ||
          receivedItems.contains(item) ||
          quarantined.contains(item.id)) {
        continue;
      }
      itemsToWrite[item.id] = item;
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
    final decided = <({String id, UpdateCompanion<Syncable> companion, bool insert})>[];
    for (final incomingItem in incomingItems.values) {
      final existingUpdatedAt = existingItems[incomingItem.id];
      if (existingUpdatedAt == null) {
        decided.add((id: incomingItem.id, companion: incomingItem.toCompanion(), insert: true));
      } else if (incomingItem.updatedAt.isAfter(existingUpdatedAt)) {
        decided.add((id: incomingItem.id, companion: incomingItem.toCompanion(), insert: false));
      }
      // else: local copy is newer — leave it (and its dirty flag) untouched so a
      // pending local edit still gets pushed.
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
          quarantined.add(write.id);
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
    List<({String id, UpdateCompanion<Syncable> companion, bool insert})> writes,
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
    });
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

enum TimestampType {
  lastSyncFromBackend('lastSyncFromBackend'),
  lastSyncToBackend('lastSyncToBackend');

  const TimestampType(this.name);
  final String name;
}
