from pathlib import Path

p = Path('lib/src/sync_manager.dart')
s = p.read_text()

def replace_once(old: str, new: str):
    global s
    count = s.count(old)
    if count != 1:
        raise SystemExit(f'expected one occurrence, found {count}: {old[:80]!r}')
    s = s.replace(old, new, 1)

def replace_between(start: str, end: str, new: str):
    global s
    a = s.find(start)
    if a < 0:
        raise SystemExit(f'start marker not found: {start!r}')
    b = s.find(end, a)
    if b < 0:
        raise SystemExit(f'end marker not found: {end!r}')
    s = s[:a] + new + s[b:]

replace_once(
"""  bool _disposed = false;
  bool _loopRunning = false;

  /// Set while the sync loop is parked in [_idle]. Completed by [_wake] to drain
""",
"""  bool _disposed = false;
  bool _loopRunning = false;

  /// A retry timer parks a table after a transient push failure. Queue occupancy
  /// alone must never mean "retry immediately" or a fast 429/5xx/network error
  /// turns the worker into a hot loop.
  final Map<Type, Timer> _outgoingRetryTimers = {};
  final Map<Type, int> _outgoingRetryAttempts = {};
  static const Duration _maxOutgoingRetryDelay = Duration(minutes: 5);

  /// Coalesces overlapping reconcile requests into the one lifetime-owned
  /// sweep. A full-resync request is sticky until the active sweep can service
  /// it, so a weaker incremental request can never swallow it.
  bool _pendingSweep = false;
  bool _pendingFullResync = false;
  String _pendingSweepReason = 'coalesced request';
  Completer<void>? _sweepCompletion;

  /// Set while the sync loop is parked in [_idle]. Completed by [_wake] to drain
""",
)

replace_once(
"""    _resubscribeTimer?.cancel();
    _resubscribeTimer = null;
    _resubscribeAttempts = 0;
    for (final subscription in _localSubscriptions.values) {
""",
"""    _resubscribeTimer?.cancel();
    _resubscribeTimer = null;
    _resubscribeAttempts = 0;
    for (final timer in _outgoingRetryTimers.values) {
      timer.cancel();
    }
    _outgoingRetryTimers.clear();
    _outgoingRetryAttempts.clear();
    for (final subscription in _localSubscriptions.values) {
""",
)

replace_between(
"  Future<void> _startLoop() async {",
"  /// Wakes the sync loop if it is currently parked in [_idle].",
"""  Future<void> _startLoop() async {
    // enable/disable is a state transition, not worker ownership. The manager
    // owns exactly one worker for its lifetime; re-enabling only wakes it.
    if (_loopRunning || _disposed) return;
    _loopRunning = true;
    _logger.info('Sync loop started');

    try {
      while (!_disposed) {
        // Incoming writes go first and outgoing work is bounded to one batch per
        // table per pass. A slow upload can therefore delay at most one bounded
        // batch before already-decoded realtime changes get another chance to
        // land locally.
        try {
          for (final syncable in _syncables) {
            if (_disposed) break;
            await _processIncoming(syncable);
          }
        } catch (e, st) {
          _logger.severe('Error processing incoming: $e\\n$st');
        }

        try {
          for (final syncable in _syncables) {
            if (_disposed) break;
            await _processOutgoing(syncable);
          }
        } catch (e, st) {
          _logger.severe('Error processing outgoing: $e\\n$st');
        }

        if (_disposed) break;
        await _idle();
      }
    } finally {
      _loopRunning = false;
      _logger.info('Sync loop stopped');
    }
  }

""",
)

replace_between(
"  Future<void> _idle() async {",
"  /// Goes through the local tables for all registered syncables",
"""  Future<void> _idle() async {
    if (_disposed) return;

    final actionableOutgoing =
        _syncingEnabled &&
        _hasLiveSession &&
        _outQueues.entries.any(
          (entry) =>
              entry.value.isNotEmpty &&
              !(_outgoingRetryTimers[entry.key]?.isActive ?? false),
        );
    if (actionableOutgoing || isSyncingFromBackend) return;

    final signal = _wakeSignal = Completer<void>();

    // When deliberately disabled (or before a user id exists), park fully.
    // enableSync/setUserId calls _onDependenciesChanged, which wakes the worker.
    Timer? backstop;
    if (_syncingEnabled) {
      backstop = Timer(_syncInterval, () {
        if (!signal.isCompleted) signal.complete();
      });
    }

    try {
      await signal.future;
    } finally {
      backstop?.cancel();
      if (identical(_wakeSignal, signal)) _wakeSignal = null;
    }
  }

""",
)

replace_once(
"""      _localSubscriptions[syncable] = _localDb.subscribe(
        table: _localTables[syncable]!,
        // GAM-389: RLS-trusting — watch ALL local rows, not just this user's,
        // so a co-member-owned row still pushes when locally edited.
        filter: (SyncableTable row) => const Constant<bool>(true),
        onChange: (rows) {
""",
"""      _localSubscriptions[syncable] = _localDb.subscribeToDirty(
        table: _localTables[syncable]!,
        // GAM-389 still applies: ownership is deliberately not part of this SQL
        // predicate. We only narrow on persistent dirty state, so co-member rows
        // edited locally still push without rematerializing clean history.
        onChange: (rows) {
""",
)

replace_once(
"""    bool updateHasNotBeenSentYet(Syncable row) =>
        row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0)) &&
        row.updatedAt.isAfter(_lastPushedTimestamp(syncable) ?? DateTime(0));
""",
"""    bool updateHasNotBeenSentYet(Syncable row) =>
        row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0));
""",
)

replace_between(
"  Future<void> _syncTables(String reason, {bool fullResync = false}) async {",
"  Future<void> _syncTable(Type syncable, {bool fullResync = false}) async {",
"""  Future<void> _syncTables(String reason, {bool fullResync = false}) async {
    if (_sweepRunning) {
      _pendingSweep = true;
      _pendingFullResync = _pendingFullResync || fullResync;
      if (fullResync || _pendingSweepReason == 'coalesced request') {
        _pendingSweepReason = reason;
      }
      await _sweepCompletion?.future;
      return;
    }

    _sweepRunning = true;
    final completion = _sweepCompletion = Completer<void>();
    Object? firstError;
    StackTrace? firstStack;

    try {
      var nextReason = reason;
      var nextFullResync = fullResync;
      do {
        _pendingSweep = false;
        _pendingFullResync = false;
        _pendingSweepReason = 'coalesced request';
        try {
          await _performSyncTables(nextReason, fullResync: nextFullResync);
        } catch (e, st) {
          firstError ??= e;
          firstStack ??= st;
        }
        nextReason = _pendingSweepReason;
        nextFullResync = _pendingFullResync;
      } while (_pendingSweep && !_disposed);

      if (firstError != null) {
        Error.throwWithStackTrace(firstError!, firstStack!);
      }
    } finally {
      _sweepRunning = false;
      _sweepCompletion = null;
      if (!completion.isCompleted) completion.complete();
    }
  }

  Future<void> _performSyncTables(
    String reason, {
    required bool fullResync,
  }) async {
    if (!__syncingEnabled) {
      _logger.info('Tables not getting synced because syncing is disabled');
      return;
    }

    if (userId.isEmpty) {
      _logger.info('Tables not getting synced because user ID is empty');
      return;
    }

    _logger.info('Syncing all tables. Reason: $reason (user: $_userId)');

    Object? firstError;
    StackTrace? firstStack;
    for (final syncable in _syncables) {
      try {
        await _syncTable(syncable, fullResync: fullResync);
      } catch (e, st) {
        firstError ??= e;
        firstStack ??= st;
        _logger.severe(
          'Sweep failed at table ${_backendTables[syncable]}',
          e,
          st,
        );
        // Tables are independent unless explicitly modeled otherwise. One bad
        // table must not starve every table registered after it.
      }
    }

    _nFullSyncs++;
    if (firstError != null) {
      Error.throwWithStackTrace(firstError!, firstStack!);
    }
  }

  Future<void> _syncTable(Type syncable, {bool fullResync = false}) async {
""",
)

replace_between(
"  Future<void> _syncTable(Type syncable, {bool fullResync = false}) async {",
"  bool _skipSyncFromBackend(Type syncable) {",
"""  Future<void> _syncTable(Type syncable, {bool fullResync = false}) async {
    final backendTable = _backendTables[syncable];
    final localTable = _localTables[syncable]!;

    if (!_syncingEnabled) {
      _logger.info('Sweep $backendTable: skipped (syncing disabled)');
      return;
    }

    // Push discovery needs complete models, but only dirty ones. Pull conflict
    // comparison only needs id + updated_at, so use a narrow projection rather
    // than materializing every content column in accumulated history.
    final dirtyColumn =
        localTable.columnsByName['dirty']! as GeneratedColumn<bool>;
    final dirtyItems = await (_localDb.select(
      localTable,
    )..where((_) => dirtyColumn.equals(true))).get();

    final idColumn = localTable.columnsByName[idKey]! as GeneratedColumn<String>;
    final updatedAtColumn =
        localTable.columnsByName[updatedAtKey]! as GeneratedColumn<DateTime>;
    final metadataRows = await (_localDb.selectOnly(localTable)
          ..addColumns([idColumn, updatedAtColumn]))
        .get();
    final localItemsUpdatedAt = <String, DateTime>{
      for (final row in metadataRows)
        row.read(idColumn)!: row.read(updatedAtColumn)!,
    };

    if (!_syncingEnabled) {
      _logger.info('Sweep $backendTable: skipped (syncing disabled mid-sweep)');
      return;
    }

    final queued = _pushLocalChangesToOutQueue(syncable, dirtyItems);
    _trackStuckRows(syncable, dirtyItems);

    if (!fullResync && _skipSyncFromBackend(syncable)) {
      _logger.info(
        'Sweep $backendTable: queued $queued for push, pull skipped '
        '(no other device active since last sync)',
      );
      return;
    }

    final pullStartedAt = DateTime.now().toUtc();
    final backendItems = await _fetchBackendItemMetadata(syncable);
    final itemsToPull = _getItemsToPullFromBackend(
      backendItems,
      localItemsUpdatedAt,
    ).toList();

    for (final batch in itemsToPull.slices(100)) {
      if (!_syncingEnabled) {
        _logger.info('Sweep $backendTable: aborted (syncing disabled mid-pull)');
        return;
      }
      final pulledBatch = await _supabaseClient
          .from(_backendTables[syncable]!)
          .select()
          .inFilter(idKey, batch);

      await Future.wait(
        pulledBatch.map((wireRow) => _enqueueIncoming(syncable, wireRow)),
      );

      // Acknowledge progress only after this page is durably applied locally.
      // This closes the crash window where the old code persisted its cursor
      // while decoded rows were still waiting in the worker queue.
      await _processIncoming(syncable);
    }

    await _updateLastPulledTimeStamp(syncable, pullStartedAt);

    _logger.info(
      'Sweep $backendTable: queued $queued for push, pulled '
      '${itemsToPull.length} of ${backendItems.length} backend candidates '
      '(full metadata reconciliation)',
    );
  }

""",
)

replace_between(
"  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(",
"  Iterable<String> _getItemsToPullFromBackend(",
"""  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(
    Type syncable, {
    bool fullResync = false,
  }) async {
    final List<Map<String, dynamic>> backendItems = [];

    // updated_at is a conflict timestamp, not a server change cursor. An
    // accepted offline edit can arrive much later while retaining an old
    // updated_at, so filtering discovery by the reader's previous pull time can
    // permanently hide a real change. Until the backend exposes a genuine
    // monotonic change cursor, reconciliation intentionally reads all visible
    // id/timestamp metadata and lets the cheap metadata comparison decide what
    // content to fetch.
    int offset = 0;
    bool hasMore = true;

    while (hasMore && _syncingEnabled) {
      final batch = await _supabaseClient
          .from(_backendTables[syncable]!)
          .select('$idKey,$updatedAtKey')
          .range(offset, offset + _maxRows - 1)
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

""",
)

replace_between(
"  Future<void> _processOutgoing(Type syncable) async {",
"  Future<void> _upsertPayloads(",
"""  bool _outgoingRetryParked(Type syncable) =>
      _outgoingRetryTimers[syncable]?.isActive ?? false;

  void _clearOutgoingRetry(Type syncable) {
    _outgoingRetryTimers.remove(syncable)?.cancel();
    _outgoingRetryAttempts.remove(syncable);
  }

  void _scheduleOutgoingRetry(Type syncable) {
    if (_disposed || _outgoingRetryParked(syncable)) return;
    final attempt = (_outgoingRetryAttempts[syncable] ?? 0) + 1;
    _outgoingRetryAttempts[syncable] = attempt;

    var delay = _syncInterval;
    for (var i = 1; i < attempt && delay < _maxOutgoingRetryDelay; i++) {
      delay *= 2;
    }
    final jittered = delay * (0.75 + _random.nextDouble() * 0.5);
    delay = jittered > _maxOutgoingRetryDelay
        ? _maxOutgoingRetryDelay
        : jittered;

    _outgoingRetryTimers[syncable] = _timerFactory(delay, () {
      _outgoingRetryTimers.remove(syncable);
      if (!_disposed) _wake();
    });
  }

  Future<void> _processOutgoing(Type syncable) async {
    if (!_syncingEnabled || !_hasLiveSession || _outgoingRetryParked(syncable)) {
      return;
    }

    final outQueue = _outQueues[syncable]!;
    if (outQueue.isEmpty) return;

    final backendTable = _backendTables[syncable]!;
    final quarantined = _outgoingQuarantined[syncable]!;

    // Snapshot then clear so quarantined/deferred rows do not keep idle() hot.
    // Requeue only eligible overflow and retryable failures.
    final snapshot = outQueue.values.toList(growable: false);
    outQueue.clear();
    final eligible = snapshot
        .where((row) {
          final failedAt = quarantined[row.id];
          return failedAt == null || row.updatedAt.isAfter(failedAt);
        })
        .where((row) => !_isEncryptionDeferred(syncable, row))
        .toList(growable: false);

    final outgoing = eligible.take(_maxRows).toSet();
    for (final row in eligible.skip(_maxRows)) {
      _enqueueKeepingNewest(outQueue, row);
    }
    if (outgoing.isEmpty) return;

    assert(!outgoing.any((row) => row.userId?.isEmpty ?? true));

    final encoded = <Syncable, Map<String, dynamic>>{};
    final encodeResults = await Future.wait(
      outgoing.map(
        (row) async =>
            (row: row, payload: await _encodeOutgoing(syncable, row)),
      ),
    );
    for (final result in encodeResults) {
      if (result.payload != null) encoded[result.row] = result.payload!;
    }
    if (encoded.isEmpty) return;

    _logger.info(
      'Syncing ${encoded.length} items to backend table $backendTable',
    );

    try {
      await _upsertPayloads(backendTable, encoded.values);
      await _markPushed(syncable, encoded.keys.toSet());
      _clearOutgoingRetry(syncable);
    } on PostgrestException catch (batchError) {
      if (_isTransientPostgrest(batchError) ||
          _isSessionlessRlsRejection(batchError)) {
        for (final row in encoded.keys) {
          _enqueueKeepingNewest(outQueue, row);
        }
        _logger.warning(
          'Re-enqueuing batch to $backendTable after a retryable rejection '
          '($batchError); backing off',
        );
        _scheduleOutgoingRetry(syncable);
        return;
      }

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
          if (_isTransientPostgrest(rowError) ||
              _isSessionlessRlsRejection(rowError)) {
            retry.add(row);
          } else {
            quarantined[row.id] = row.updatedAt;
            _logger.severe(
              'Quarantined poison row ${row.id} in $backendTable after '
              'backend rejection: $rowError\\n$rowStack',
            );
          }
        } catch (_) {
          retry.add(row);
        }
      }

      await _markPushed(syncable, succeeded);
      if (succeeded.isNotEmpty) _clearOutgoingRetry(syncable);

      if (retry.isNotEmpty) {
        for (final row in retry) {
          _enqueueKeepingNewest(outQueue, row);
        }
        _scheduleOutgoingRetry(syncable);
      }
    } catch (batchError) {
      for (final row in encoded.keys) {
        _enqueueKeepingNewest(outQueue, row);
      }
      _logger.warning(
        'Transient failure pushing to $backendTable ($batchError); '
        'backing off',
      );
      _scheduleOutgoingRetry(syncable);
    }
  }

""",
)

p.write_text(s)

# Replace tests that asserted the unsafe watermark behavior with correctness
# assertions for full metadata reconciliation and add a lifecycle/backoff test.
t = Path('test/sync_manager_test.dart')
ts = t.read_text()
start = ts.find("  // MC-413 item 4: incremental metadata fetch.")
end = ts.find("  group(", start + 20)
if start < 0 or end < 0:
    raise SystemExit('incremental metadata test group markers not found')
# Find the next group after the incremental group by balancing braces.
group_start = ts.find("  group('Incremental metadata fetch'", start)
if group_start < 0:
    raise SystemExit('incremental group start not found')
pos = group_start
depth = 0
in_string = False
quote = None
escaped = False
seen_open = False
while pos < len(ts):
    ch = ts[pos]
    if in_string:
        if escaped:
            escaped = False
        elif ch == '\\':
            escaped = True
        elif ch == quote:
            in_string = False
    elif ch in "'\"":
        in_string = True
        quote = ch
    elif ch == '(':
        depth += 1
        seen_open = True
    elif ch == ')':
        depth -= 1
        if seen_open and depth == 0:
            semi = ts.find(';', pos)
            if semi < 0:
                raise SystemExit('group terminator missing')
            group_end = semi + 1
            break
    pos += 1
else:
    raise SystemExit('could not find incremental group end')

replacement = r'''  // updated_at is conflict metadata, not an arrival cursor. Reconciliation
  // therefore always fetches the complete RLS-visible id/timestamp projection.
  group('Reconciliation metadata discovery', () {
    List<Uri> metadataGetUris() {
      final captured = verify(
        mockHttpClient.get(captureAny, headers: anyNamed('headers')),
      ).captured.cast<Uri>();
      return captured.where((uri) => uri.query.contains('select=id%2Cupdated_at')).toList();
    }

    test('normal sweeps never filter discovery by updated_at', () async {
      final storage = TimestampStorage();
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        syncTimestampStorage: storage,
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      await syncManager.syncTables();
      await syncManager.syncTables();

      final uris = metadataGetUris();
      expect(uris, isNotEmpty);
      expect(
        uris.every((uri) => !uri.query.contains('updated_at=gt.')),
        isTrue,
        reason: 'late accepted offline writes must remain discoverable',
      );
      syncManager.dispose();
    });
  });'''
ts = ts[:start] + replacement + ts[group_end:]

# Add a deterministic retry/lifecycle regression before the first outgoing
# quarantine test. FakeTimerFactory is already part of this suite.
anchor = "  test('an outgoing-quarantined (un-pushable) row is still pullable"
idx = ts.find(anchor)
if idx < 0:
    raise SystemExit('retry test insertion anchor not found')
retry_test = r'''  test('transient push is timer-backed and re-enable does not create a second worker', () async {
    final timerFactory = FakeTimerFactory();
    var attempts = 0;
    when(
      mockHttpClient.post(
        any,
        headers: anyNamed('headers'),
        body: anyNamed('body'),
        encoding: anyNamed('encoding'),
      ),
    ).thenAnswer((inv) async {
      attempts++;
      if (attempts == 1) {
        return Response(
          jsonEncode({
            'code': '503',
            'message': 'service temporarily unavailable',
            'details': null,
            'hint': null,
          }),
          503,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }
      return Response(
        inv.namedArguments[#body] as String,
        200,
        request: Request('POST', Uri()),
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(seconds: 5),
      random: FixedRandom(0.5),
      timerFactory: timerFactory.call,
    );
    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );
    final userId = const Uuid().v4();
    syncManager.setUserId(userId);
    syncManager.enableSync();

    await testDb.into(testDb.items).insert(
      ItemsCompanion(
        id: drift.Value(const Uuid().v4()),
        userId: drift.Value(userId),
        updatedAt: drift.Value(DateTime.now()),
        deleted: const drift.Value(false),
        name: const drift.Value('retry me'),
      ),
    );

    await waitForFunctionToPass(() async {
      expect(attempts, 1);
      expect(timerFactory.timers.where((timer) => timer.isActive).length, 1);
    });

    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(attempts, 1, reason: 'queued row must stay parked until retry timer fires');

    syncManager.disableSync();
    syncManager.enableSync();
    expect(attempts, 1, reason: 're-enable must not start another worker');

    final retryTimer = timerFactory.timers.firstWhere((timer) => timer.isActive);
    retryTimer.fire();
    await waitForFunctionToPass(() async => expect(attempts, 2));
    syncManager.dispose();
  });

'''
ts = ts[:idx] + retry_test + ts[idx:]
t.write_text(ts)
