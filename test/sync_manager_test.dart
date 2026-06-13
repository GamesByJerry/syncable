import 'dart:async';
import 'dart:convert';

import 'package:drift/drift.dart' as drift;
import 'package:drift/native.dart' as drift_native;
import 'package:http/http.dart';
import 'package:logging/logging.dart';
import 'package:mockito/mockito.dart';
import 'package:supabase/supabase.dart';
import 'package:syncable/src/supabase_names.dart';
import 'package:syncable/syncable.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import 'utils/test_database.dart';
import 'utils/test_mocks.mocks.dart';
import 'utils/test_supabase_names.dart';
import 'utils/wait_for_function_to_pass.dart';

class TimestampStorage extends SyncTimestampStorage {
  final Map<String, DateTime> _timestamps = {};

  @override
  Future<void> setSyncTimestamp(String key, DateTime timestamp) async {
    _timestamps[key] = timestamp;
  }

  @override
  DateTime? getSyncTimestamp(String key) {
    return _timestamps[key];
  }
}

/// Hands back watermarks as *local* (non-UTC) DateTimes — a plausible custom
/// storage (e.g. one that parses without forcing UTC). Used to prove the
/// incremental metadata filter still serializes as UTC.
class LocalReturningTimestampStorage extends TimestampStorage {
  @override
  DateTime? getSyncTimestamp(String key) =>
      super.getSyncTimestamp(key)?.toLocal();
}

void main() {
  late TestDatabase testDb;

  late MockSupabaseClient mockSupabaseClient;
  late MockSupabaseQueryBuilder mockQueryBuilder;
  late MockClient mockHttpClient;
  late MockRealtimeChannel mockRealtimeChannel;
  late MockGoTrueClient mockGoTrue;
  late MockSession mockSession;
  late StreamController<AuthState> authEvents;

  setUp(() {
    testDb = TestDatabase(
      drift.DatabaseConnection(
        drift_native.NativeDatabase.memory(),
        closeStreamsSynchronously: true,
      ),
    );

    // Set up mocks for Supabase to allow upserting things
    mockSupabaseClient = MockSupabaseClient();
    mockQueryBuilder = MockSupabaseQueryBuilder();
    mockHttpClient = MockClient();

    // Auth context (MC-442): the engine refuses to push while there is no live
    // Supabase session, since session-less pushes go out as `anon` and RLS
    // rejects valid rows with 42501. Default to a LIVE session so the existing
    // sync tests behave as before; the session-gate tests below override this.
    mockGoTrue = MockGoTrueClient();
    mockSession = MockSession();
    authEvents = StreamController<AuthState>.broadcast();
    when(mockSupabaseClient.auth).thenReturn(mockGoTrue);
    when(mockGoTrue.currentSession).thenReturn(mockSession);
    when(mockGoTrue.onAuthStateChange).thenAnswer((_) => authEvents.stream);
    // mockSession.isExpired defaults to false (NiceMock bool) → a live session.

    final realQueryBuilder = PostgrestQueryBuilder(
      url: Uri(),
      httpClient: mockHttpClient,
    );

    when(
      mockSupabaseClient.from(itemsTable),
    ).thenAnswer((_) => mockQueryBuilder);
    when(
      mockQueryBuilder.upsert(any, onConflict: anyNamed('onConflict')),
    ).thenAnswer(
      (inv) => realQueryBuilder.upsert(
        inv.positionalArguments[0] as Object,
        onConflict: inv.namedArguments[#onConflict] as String?,
      ),
    );
    when(
      mockHttpClient.post(
        any,
        headers: anyNamed('headers'),
        body: anyNamed('body'),
      ),
    ).thenAnswer(
      (_) async => Response(
        jsonEncode([
          {idKey: 'abc'},
        ]),
        200,
        request: Request('POST', Uri()),
      ),
    );
    when(mockQueryBuilder.select(any)).thenAnswer(
      (inv) => realQueryBuilder.select(inv.positionalArguments[0] as String),
    );
    when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
      (_) async =>
          Response(jsonEncode([]), 200, request: Request('GET', Uri())),
    );

    // Set up mocks for Supabase to allow listening to changes in the database
    mockRealtimeChannel = MockRealtimeChannel();
    when(mockSupabaseClient.channel(any)).thenReturn(mockRealtimeChannel);
    when(
      mockRealtimeChannel.onPostgresChanges(
        schema: anyNamed('schema'),
        table: anyNamed('table'),
        event: anyNamed('event'),
        callback: anyNamed('callback'),
      ),
    ).thenReturn(mockRealtimeChannel);
    when(
      mockRealtimeChannel.subscribe(any),
    ).thenAnswer((_) => mockRealtimeChannel);
  });

  tearDown(() async {
    await authEvents.close();
    await testDb.close();
  });

  test('Newly created items get sent to backend', () async {
    final syncManager = SyncManager(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    final userId = const Uuid().v4();

    syncManager.enableSync();
    syncManager.setUserId(userId);

    expect(syncManager.nSyncedToBackend(Item), 0);

    final rowId = await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            userId: drift.Value(userId),
            updatedAt: drift.Value(DateTime.now()),
            deleted: const drift.Value(false),
            name: const drift.Value('Test Item'),
          ),
        );

    final item = await (testDb.select(
      testDb.items,
    )..where((tbl) => tbl.rowId.equals(rowId))).getSingle();

    await waitForFunctionToPass(() async {
      expect(syncManager.nSyncedToBackend(Item), 1);
    });

    verify(
      mockQueryBuilder.upsert([
        item.toJson(),
      ], onConflict: anyNamed('onConflict')),
    ).called(1);
  });

  test('a backend-rejected poison row is isolated; the rest still flush '
      'and the poison row is preserved (per-row fallback)', () async {
    const poisonId = 'poison-row-id';

    // Reject any upsert whose body contains the poison row (simulates a
    // constraint / RLS violation that previously wedged the whole batch).
    when(
      mockHttpClient.post(
        any,
        headers: anyNamed('headers'),
        body: anyNamed('body'),
      ),
    ).thenAnswer((inv) async {
      final body = inv.namedArguments[#body] as String;
      final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
      if (rows.any((r) => r[idKey] == poisonId)) {
        return Response(
          jsonEncode({
            'code': '23505',
            'message': 'duplicate key value violates unique constraint',
            'details': null,
            'hint': null,
          }),
          409,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }
      return Response(
        jsonEncode(rows),
        200,
        request: Request('POST', Uri()),
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );
    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    final userId = const Uuid().v4();
    syncManager.enableSync();
    syncManager.setUserId(userId);

    // A poison row (always rejected) and a healthy row queued together.
    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            id: const drift.Value(poisonId),
            userId: drift.Value(userId),
            updatedAt: drift.Value(DateTime.now()),
            deleted: const drift.Value(false),
            name: const drift.Value('Poison'),
          ),
        );
    final goodId = const Uuid().v4();
    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            id: drift.Value(goodId),
            userId: drift.Value(userId),
            updatedAt: drift.Value(DateTime.now()),
            deleted: const drift.Value(false),
            name: const drift.Value('Good'),
          ),
        );

    // The healthy row flushes (dirty cleared) despite the poison row sharing
    // the table — no permanent wedge.
    await waitForFunctionToPass(() async {
      final good = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(goodId))).getSingle();
      expect(good.dirty, isFalse, reason: 'healthy row flushed past poison');
    });

    // The poison row is preserved locally with dirty=true — quarantined, not
    // lost. (This is the unsynced-data-safety guarantee.)
    final poison = await (testDb.select(
      testDb.items,
    )..where((t) => t.id.equals(poisonId))).getSingle();
    expect(poison.dirty, isTrue, reason: 'poison row kept, not dropped');

    syncManager.dispose();
  });

  test(
    'a transient backend failure (5xx/429) is retried, never quarantined',
    () async {
      // The backend fails the push transiently (503) on its first attempts, then
      // recovers. A transient failure must NOT quarantine the row — that would
      // suppress it until app restart over a passing hiccup. It must keep being
      // retried and flush once the backend is healthy again (MC-424 §B/§C).
      // PostgREST reports rate limits / server errors with the HTTP status in
      // `code`, NOT a Postgres SQLSTATE — that is what marks it transient.
      var attempts = 0;
      when(
        mockHttpClient.post(
          any,
          headers: anyNamed('headers'),
          body: anyNamed('body'),
        ),
      ).thenAnswer((inv) async {
        final body = inv.namedArguments[#body] as String;
        final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
        attempts++;
        if (attempts <= 2) {
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
          jsonEncode(rows),
          200,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      });

      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final id = const Uuid().v4();
      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion(
              id: drift.Value(id),
              userId: drift.Value(userId),
              updatedAt: drift.Value(DateTime.now()),
              deleted: const drift.Value(false),
              name: const drift.Value('Flaky'),
            ),
          );

      // It eventually flushes once the backend recovers — proving it was retried
      // past the 503s, not quarantined after the first failure.
      await waitForFunctionToPass(() async {
        final row = await (testDb.select(
          testDb.items,
        )..where((t) => t.id.equals(id))).getSingle();
        expect(
          row.dirty,
          isFalse,
          reason: 'transient row recovered and flushed',
        );
      });
      expect(
        attempts,
        greaterThan(2),
        reason: 'row was retried past the transient 503s',
      );

      syncManager.dispose();
    },
  );

  test('an outgoing-quarantined (un-pushable) row is still pullable — the '
      'outgoing quarantine must not suppress incoming', () async {
    const poisonId = 'out-poison-still-pullable-id';

    // The backend permanently rejects any PUSH carrying the poison row, so it
    // lands in the OUTGOING quarantine.
    when(
      mockHttpClient.post(
        any,
        headers: anyNamed('headers'),
        body: anyNamed('body'),
      ),
    ).thenAnswer((inv) async {
      final body = inv.namedArguments[#body] as String;
      final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
      if (rows.any((r) => r[idKey] == poisonId)) {
        return Response(
          jsonEncode({
            'code': '23505',
            'message': 'duplicate key value violates unique constraint',
            'details': null,
            'hint': null,
          }),
          409,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }
      return Response(
        jsonEncode(rows),
        200,
        request: Request('POST', Uri()),
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );
    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    final userId = const Uuid().v4();
    syncManager.enableSync();
    syncManager.setUserId(userId);

    // A local dirty poison row (v1) that can't be pushed.
    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            id: const drift.Value(poisonId),
            userId: drift.Value(userId),
            updatedAt: drift.Value(
              DateTime.now().subtract(const Duration(minutes: 1)),
            ),
            deleted: const drift.Value(false),
            name: const drift.Value('Poison v1'),
          ),
        );

    // The backend holds a NEWER version of the SAME id — e.g. another device
    // fixed it. With a shared quarantine this pull would be dropped; with the
    // outgoing/incoming split it must land.
    final backendV2 = Item(
      id: poisonId,
      userId: userId,
      updatedAt: DateTime.now().add(const Duration(minutes: 1)),
      deleted: false,
      name: 'Backend v2',
    );
    when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
      (_) async => Response(
        jsonEncode([backendV2.toJson()]),
        200,
        request: Request('GET', Uri()),
      ),
    );

    // Drive sync explicitly (as the other pull tests do). Despite the push
    // staying quarantined, the newer backend version is pulled and written
    // over the local row — proving the outgoing quarantine doesn't suppress
    // the incoming pull.
    await waitForFunctionToPass(() async {
      await syncManager.syncTables();
      final row = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(poisonId))).getSingle();
      expect(
        row.name,
        'Backend v2',
        reason: 'incoming pull must not be blocked by the outgoing quarantine',
      );
      expect(row.dirty, isFalse, reason: 'pulled row is clean');
    });

    syncManager.dispose();
  });

  test('a quarantined row is retried once its version moves on — the quarantine '
      'is versioned, not permanent', () async {
    const id = 'versioned-quarantine-id';
    var poisonRejections = 0;

    // The backend rejects the row while its name is still 'Poison'; it accepts
    // once the row has been edited (the fix). The id-only quarantine would keep
    // skipping it forever; the versioned quarantine lets the newer edit retry.
    when(
      mockHttpClient.post(
        any,
        headers: anyNamed('headers'),
        body: anyNamed('body'),
      ),
    ).thenAnswer((inv) async {
      final body = inv.namedArguments[#body] as String;
      final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
      final poisoned = rows.any(
        (r) => r[idKey] == id && (r['name'] as String?) == 'Poison',
      );
      if (poisoned) {
        poisonRejections++;
        return Response(
          jsonEncode({
            'code': '23505',
            'message': 'duplicate key value violates unique constraint',
            'details': null,
            'hint': null,
          }),
          409,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }
      return Response(
        jsonEncode(rows),
        200,
        request: Request('POST', Uri()),
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );
    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );
    final userId = const Uuid().v4();
    syncManager.enableSync();
    syncManager.setUserId(userId);

    // v1: poison — the push is rejected and the row is quarantined at v1.
    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            id: const drift.Value(id),
            userId: drift.Value(userId),
            updatedAt: drift.Value(DateTime.now()),
            deleted: const drift.Value(false),
            name: const drift.Value('Poison'),
          ),
        );

    // Ensure the poison push was attempted (and thus quarantined) first.
    await waitForFunctionToPass(() async {
      expect(poisonRejections, greaterThanOrEqualTo(1));
    });

    // A local edit (newer updatedAt) that fixes the row. The versioned
    // quarantine must let this strictly-newer version retry and flush.
    await (testDb.update(testDb.items)..where((t) => t.id.equals(id))).write(
      ItemsCompanion(
        name: const drift.Value('Fixed'),
        updatedAt: drift.Value(DateTime.now().add(const Duration(seconds: 1))),
        dirty: const drift.Value(true),
      ),
    );

    await waitForFunctionToPass(() async {
      final row = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(id))).getSingle();
      expect(
        row.dirty,
        isFalse,
        reason: 'the edited (newer) row retried past quarantine and flushed',
      );
    });

    syncManager.dispose();
  });

  test(
    'Only subscribes to backend changes if other devices are active',
    () async {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        otherDevicesConsideredInactiveAfter: const Duration(seconds: 1),
      );

      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      // Switch device presence back and forth to make sure that the subscription
      // gets added and removed as a response

      syncManager.setLastTimeOtherDeviceWasActive(DateTime.now().toUtc());
      await waitForFunctionToPass(() async {
        expect(syncManager.isSubscribedToBackend, isTrue);
      });

      syncManager.setLastTimeOtherDeviceWasActive(
        DateTime.now().subtract(const Duration(seconds: 2)).toUtc(),
      );
      await waitForFunctionToPass(() async {
        expect(syncManager.isSubscribedToBackend, isFalse);
      });

      syncManager.setLastTimeOtherDeviceWasActive(null);
      await waitForFunctionToPass(() async {
        expect(syncManager.isSubscribedToBackend, isTrue);
      });

      syncManager.setLastTimeOtherDeviceWasActive(
        DateTime.now().subtract(const Duration(seconds: 2)).toUtc(),
      );
      await waitForFunctionToPass(() async {
        expect(syncManager.isSubscribedToBackend, isFalse);
      });

      syncManager.setLastTimeOtherDeviceWasActive(DateTime.now().toUtc());
      await waitForFunctionToPass(() async {
        expect(syncManager.isSubscribedToBackend, isTrue);
      });
    },
  );

  test(
    'Skips explicit sync to backend if unnecessary due to sync timestamps',
    () async {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        otherDevicesConsideredInactiveAfter: const Duration(seconds: 1),
        syncTimestampStorage: TimestampStorage(),
      );

      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      expect(syncManager.nSyncedFromBackend(Item), 0);

      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      final itemRowId = await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion(
              userId: drift.Value(syncManager.userId),
              updatedAt: drift.Value(DateTime.now()),
              deleted: const drift.Value(false),
              name: const drift.Value('Local item 1'),
            ),
          );

      await waitForFunctionToPass(() async {
        expect(syncManager.nFullSyncs, 1);
        expect(syncManager.nSyncedToBackend(Item), 1);
      });

      await syncManager.syncTables();

      // Check that the item was not sent again despite explicit sync
      expect(syncManager.nFullSyncs, 2);
      expect(syncManager.isSyncingToBackend, isFalse);
      expect(syncManager.nSyncedToBackend(Item), 1);

      // Update the item to trigger a sync. A local edit must mark the row
      // dirty=true (the app gets this via Syncable.toCompanion()); without it
      // the dirty-flag engine correctly treats the row as already in sync.
      await (testDb.update(
        testDb.items,
      )..where((i) => i.rowId.equals(itemRowId))).write(
        ItemsCompanion(
          updatedAt: drift.Value(DateTime.now()),
          dirty: const drift.Value(true),
        ),
      );

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(Item), 2);
      });
    },
  );

  test(
    'A restart does not re-push pulled rows; only dirty local rows push',
    () async {
      // A co-member's row pulled in a PREVIOUS session: it sits in the local DB
      // dirty=false and is owned by another user. (This is the circle row a
      // non-owner member pulls — re-pushing it fails RLS with 42501.)
      final pulledId = const Uuid().v4();
      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion.insert(
              id: drift.Value(pulledId),
              userId: drift.Value(const Uuid().v4()),
              updatedAt: DateTime.now(),
              deleted: const drift.Value(false),
              dirty: const drift.Value(false),
              name: 'pulled co-member row',
            ),
          );

      final myUserId = const Uuid().v4();
      // A genuine local edit by us: dirty=true.
      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion.insert(
              id: drift.Value(const Uuid().v4()),
              userId: drift.Value(myUserId),
              updatedAt: DateTime.now(),
              deleted: const drift.Value(false),
              dirty: const drift.Value(true),
              name: 'my local row',
            ),
          );

      // Fresh SyncManager == app restart: the in-memory received/sent sets that
      // used to guard against echoing pulled rows are empty. Only the persistent
      // dirty flag can prevent the re-push now.
      final syncManager = SyncManager(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      syncManager.setUserId(myUserId);
      syncManager.enableSync();

      // Only the dirty row is ever pushed.
      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(Item), 1);
      });

      // ...and it stays 1 across an explicit re-sync — the pulled row never goes.
      await syncManager.syncTables();
      expect(syncManager.nSyncedToBackend(Item), 1);

      // After a successful push the local row is marked clean; the pulled row
      // was already clean. So nothing is left dirty to re-push.
      final rows = await testDb.select(testDb.items).get();
      expect(rows.length, 2);
      expect(rows.every((r) => r.dirty == false), isTrue);
    },
  );

  test(
    'Skips explicit sync from backend if unnecessary due to sync timestamps',
    () async {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        otherDevicesConsideredInactiveAfter: const Duration(seconds: 1),
        syncTimestampStorage: TimestampStorage(),
      );

      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      expect(syncManager.nSyncedFromBackend(Item), 0);

      final backendItem1 = Item(
        id: const Uuid().v4(),
        userId: syncManager.userId,
        updatedAt: DateTime.now(),
        deleted: false,
        name: 'Backend item 1',
      );

      when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
        (_) async => Response(
          jsonEncode([backendItem1.toJson()]),
          200,
          request: Request('GET', Uri()),
        ),
      );

      await waitForFunctionToPass(() async {
        expect(syncManager.nFullSyncs, 1);
        expect(syncManager.nSyncedFromBackend(Item), 1);
      });

      await syncManager.syncTables();

      // Check that the item was pulled again despite explicit sync
      expect(syncManager.nFullSyncs, 2);
      expect(syncManager.isSyncingFromBackend, isFalse);
      expect(syncManager.nSyncedFromBackend(Item), 1);

      final backendItem2 = Item(
        id: const Uuid().v4(),
        userId: syncManager.userId,
        updatedAt: DateTime.now(),
        deleted: false,
        name: 'Backend item 2',
      );

      when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
        (_) async => Response(
          jsonEncode([backendItem1.toJson(), backendItem2.toJson()]),
          200,
          request: Request('GET', Uri()),
        ),
      );

      // Setting a timestamp far in the past for the last time a device was active
      // must prevent a new sync
      syncManager.setLastTimeOtherDeviceWasActive(
        DateTime.now().subtract(const Duration(days: 2)).toUtc(),
      );

      await waitForFunctionToPass(() async {
        // Check that the item was pulled again despite explicit sync
        expect(syncManager.nFullSyncs, 3);
        expect(syncManager.isSyncingFromBackend, isFalse);
        expect(syncManager.nSyncedFromBackend(Item), 1);
      });

      // Setting a timestamp now for the last time a device was active
      // must trigger a new sync
      syncManager.setLastTimeOtherDeviceWasActive(DateTime.now().toUtc());
      await waitForFunctionToPass(() async {
        expect(syncManager.nFullSyncs, 4);
        expect(syncManager.isSyncingFromBackend, isFalse);
        expect(syncManager.nSyncedFromBackend(Item), 2);
      });
    },
  );

  test('Items received from backend are not sent back', () async {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
      otherDevicesConsideredInactiveAfter: const Duration(seconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    syncManager.setUserId(const Uuid().v4());
    final backendItem1 = Item(
      id: const Uuid().v4(),
      userId: syncManager.userId,
      updatedAt: DateTime.now(),
      deleted: false,
      name: 'Backend item 1',
    );

    when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
      (_) async => Response(
        jsonEncode([backendItem1.toJson()]),
        200,
        request: Request('GET', Uri()),
      ),
    );

    syncManager.enableSync();

    for (final _ in List.generate(10, (_) => 0)) {
      expect(syncManager.isSyncingToBackend, isFalse);
      await Future.delayed(Duration.zero);
    }
  });

  test('Fill missing user ID for local tables', () async {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            updatedAt: drift.Value(DateTime.now()),
            name: const drift.Value('Test Item'),
          ),
        );

    expect((await testDb.select(testDb.items).getSingle()).userId, isNull);

    final userId = const Uuid().v4();
    syncManager.setUserId(userId);
    await syncManager.fillMissingUserIdForLocalTables();

    expect((await testDb.select(testDb.items).getSingle()).userId, userId);

    syncManager.enableSync();

    await waitForFunctionToPass(() async {
      expect(syncManager.nSyncedToBackend(Item), 1);
    });
  });

  test('Purge non-UUID-owned rows removes only invalid-owner rows', () async {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    final realUserId = const Uuid().v4();
    // 36 chars (satisfies the userId length constraint) but not a UUID — the
    // 'g' characters are non-hex, so it stands in for an offline-guest id.
    const guestUserId = 'gggggggg-gggg-gggg-gggg-gggggggggggg';

    Future<String> insert(String name, drift.Value<String?> userId) async {
      final row = await testDb
          .into(testDb.items)
          .insertReturning(
            ItemsCompanion(
              updatedAt: drift.Value(DateTime.now()),
              name: drift.Value(name),
              userId: userId,
            ),
          );
      return row.id;
    }

    final realId = await insert('real', drift.Value(realUserId));
    final guestId = await insert('guest', const drift.Value(guestUserId));
    final orphanId = await insert('orphan', const drift.Value(null));

    final removed = await syncManager.purgeNonUuidOwnedRows();

    expect(removed, 1);
    final remaining = (await testDb.select(testDb.items).get())
        .map((r) => r.id)
        .toSet();
    // Real (UUID) and null-owner rows survive; only the guest row is purged.
    expect(remaining, containsAll(<String>[realId, orphanId]));
    expect(remaining, isNot(contains(guestId)));
  });

  test('Purge handles more doomed rows than SQLite allows variables', () async {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    const guestUserId = 'gggggggg-gggg-gggg-gggg-gggggggggggg';
    // More than SQLite's ~999 bound-variable limit, so a single isIn() would
    // throw 'too many SQL variables'; the chunked delete must not.
    const doomedCount = 1500;
    await testDb.batch((batch) {
      batch.insertAll(
        testDb.items,
        List.generate(
          doomedCount,
          (i) => ItemsCompanion(
            updatedAt: drift.Value(DateTime.now()),
            name: drift.Value('guest_$i'),
            userId: const drift.Value(guestUserId),
          ),
        ),
      );
    });

    final removed = await syncManager.purgeNonUuidOwnedRows();

    expect(removed, doomedCount);
    expect(await testDb.select(testDb.items).get(), isEmpty);
  });

  test(
    'Trying to fill missing user IDs without first setting a user ID does not crash',
    () async {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
      );

      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion(
              updatedAt: drift.Value(DateTime.now()),
              name: const drift.Value('Test Item'),
            ),
          );

      expect((await testDb.select(testDb.items).getSingle()).userId, isNull);

      await syncManager.fillMissingUserIdForLocalTables();

      expect((await testDb.select(testDb.items).getSingle()).userId, isNull);
    },
  );

  test('Enabling syncing without registering syncables raises exception', () {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    expect(() => syncManager.enableSync(), throwsException);
  });

  test('Registering syncable without generic parameter raises exception', () {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );
    expect(
      () => syncManager.registerSyncable(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      ),
      throwsException,
    );
  });

  test('Registering syncable after starting sync raises exception', () {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    syncManager.enableSync();

    expect(
      () => syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      ),
      throwsException,
    );
  });

  test('Exposes registered syncables', () {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
    );

    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );

    expect(syncManager.syncables, equals([Item]));
  });

  // MC-413 item 1: event-driven queue draining. The loop must drain on a wake
  // signal from the two enqueue sites, not only on its backstop timer. A long
  // `syncInterval` makes the distinction observable: if the loop still relied on
  // the timer, these would not finish until 10s; the wake must drain in ~ms.
  group('Event-driven wake (long sync interval)', () {
    test(
      'A local change wakes the loop and pushes well under the interval',
      () async {
        final syncManager = SyncManager(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(seconds: 10),
        );
        syncManager.registerSyncable<Item>(
          backendTable: itemsTable,
          fromJson: Item.fromJson,
          companionConstructor: ItemsCompanion.new,
        );
        final userId = const Uuid().v4();
        syncManager.enableSync();
        syncManager.setUserId(userId);

        await testDb
            .into(testDb.items)
            .insert(
              ItemsCompanion(
                userId: drift.Value(userId),
                updatedAt: drift.Value(DateTime.now()),
                deleted: const drift.Value(false),
                name: const drift.Value('woken'),
              ),
            );

        await waitForFunctionToPass(
          () async => expect(syncManager.nSyncedToBackend(Item), 1),
          timeout: const Duration(seconds: 2),
        );

        syncManager.dispose();
      },
    );

    test(
      'A realtime event wakes the loop and writes locally under the interval',
      () async {
        void Function(PostgresChangePayload)? pgCallback;
        when(
          mockRealtimeChannel.onPostgresChanges(
            schema: anyNamed('schema'),
            table: anyNamed('table'),
            event: anyNamed('event'),
            callback: anyNamed('callback'),
          ),
        ).thenAnswer((inv) {
          pgCallback =
              inv.namedArguments[#callback]
                  as void Function(PostgresChangePayload)?;
          return mockRealtimeChannel;
        });

        final syncManager = SyncManager(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(seconds: 10),
        );
        syncManager.registerSyncable<Item>(
          backendTable: itemsTable,
          fromJson: Item.fromJson,
          companionConstructor: ItemsCompanion.new,
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        // The backend subscription is created once dependencies settle; that is
        // where our realtime callback gets registered.
        await waitForFunctionToPass(() async => expect(pgCallback, isNotNull));

        final incoming = Item(
          id: const Uuid().v4(),
          userId: userId,
          updatedAt: DateTime.now(),
          deleted: false,
          name: 'from-realtime',
        );
        pgCallback!(
          PostgresChangePayload(
            schema: 'public',
            table: itemsTable,
            commitTimestamp: DateTime.now(),
            eventType: PostgresChangeEvent.insert,
            newRecord: incoming.toJson(),
            oldRecord: const {},
            errors: null,
          ),
        );

        await waitForFunctionToPass(() async {
          final row = await (testDb.select(
            testDb.items,
          )..where((t) => t.id.equals(incoming.id))).getSingleOrNull();
          expect(row?.name, 'from-realtime');
        }, timeout: const Duration(seconds: 2));

        syncManager.dispose();
      },
    );

    test('Rapid successive local changes all push (no missed wakes)', () async {
      final syncManager = SyncManager(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(seconds: 10),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      // Insert many rows back-to-back, yielding between each so some land right
      // as a drain finishes — the window where a naive wake-signal would be
      // missed. The `_idle` queue re-check must still drain every one.
      const count = 25;
      for (var i = 0; i < count; i++) {
        await testDb
            .into(testDb.items)
            .insert(
              ItemsCompanion(
                userId: drift.Value(userId),
                updatedAt: drift.Value(DateTime.now()),
                deleted: const drift.Value(false),
                name: drift.Value('row_$i'),
              ),
            );
        await Future<void>.delayed(Duration.zero);
      }

      await waitForFunctionToPass(
        () async => expect(syncManager.nSyncedToBackend(Item), count),
        timeout: const Duration(seconds: 3),
      );

      syncManager.dispose();
    });
  });

  // MC-413 item 4: incremental metadata fetch. The reconcile's id/updated_at
  // sweep must be bounded to rows changed since the last pull, not the whole
  // table — except the first pull (null watermark), which stays a full sweep.
  group('Incremental metadata fetch', () {
    List<Uri> metadataGetUris() {
      final captured = verify(
        mockHttpClient.get(captureAny, headers: anyNamed('headers')),
      ).captured.cast<Uri>();
      // The metadata sweep is the only query selecting `id,updated_at`.
      return captured.where((u) => u.query.contains('select=id')).toList();
    }

    test('First sweep is full; later sweeps filter on updated_at', () async {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        syncTimestampStorage: TimestampStorage(),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );

      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      // Force reconciles explicitly: the package loop only drains queues; full
      // syncs come from dependency changes / explicit calls (the heartbeat that
      // would drive them periodically lives in the app, not the package).
      await syncManager.syncTables();
      await syncManager.syncTables();
      syncManager.dispose();

      final metaUris = metadataGetUris();
      expect(metaUris.length, greaterThanOrEqualTo(2));
      // First pull: no stored watermark yet → unfiltered full sweep.
      expect(metaUris.first.query, isNot(contains('updated_at=gt')));
      // A later pull, once a watermark exists, is bounded by updated_at.
      expect(
        metaUris.any((u) => u.query.contains('updated_at=gt.')),
        isTrue,
        reason: 'expected an incremental sweep filtered on updated_at',
      );
    });

    test(
      'Incremental filter is serialized as UTC even if storage is local',
      () async {
        // Guards the toUtc() on the watermark: a local DateTime would otherwise
        // serialize without the 'Z' the backend needs (silent tz mismatch).
        final syncManager = SyncManager<TestDatabase>(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(milliseconds: 1),
          syncTimestampStorage: LocalReturningTimestampStorage(),
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
        syncManager.dispose();

        final gtUris = metadataGetUris()
            .where((u) => u.query.contains('updated_at=gt.'))
            .toList();
        expect(gtUris, isNotEmpty);
        for (final u in gtUris) {
          final raw = Uri.decodeComponent(
            u.query.split('updated_at=gt.')[1].split('&').first,
          );
          expect(
            raw.endsWith('Z'),
            isTrue,
            reason: 'incremental filter not UTC-serialized: $raw',
          );
        }
      },
    );

    test(
      'A full-resync sweep ignores the watermark even when one exists',
      () async {
        // When the set of rows a client may read GROWS for a non-temporal
        // reason — e.g. joining a circle whose rows (and the circle row itself)
        // were last modified BEFORE this client's last pull — the incremental
        // `updated_at > watermark` filter silently skips them: they are older
        // than the watermark yet only just became visible. A forced full
        // resync must drop the filter so those pre-existing rows are pulled.
        final syncManager = SyncManager<TestDatabase>(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(milliseconds: 1),
          syncTimestampStorage: TimestampStorage(),
        );
        syncManager.registerSyncable<Item>(
          backendTable: itemsTable,
          fromJson: Item.fromJson,
          companionConstructor: ItemsCompanion.new,
        );

        syncManager.setUserId(const Uuid().v4());
        syncManager.enableSync();

        // First sweep establishes a watermark; the second is incremental
        // (filtered on updated_at) — proving a watermark now exists.
        await syncManager.syncTables();
        await syncManager.syncTables();
        // The forced full resync must NOT carry the updated_at filter despite
        // the stored watermark.
        await syncManager.syncTables(fullResync: true);
        syncManager.dispose();

        final metaUris = metadataGetUris();
        expect(
          metaUris.any((u) => u.query.contains('updated_at=gt.')),
          isTrue,
          reason: 'a watermark-bounded incremental sweep must have happened',
        );
        expect(
          metaUris.last.query,
          isNot(contains('updated_at=gt')),
          reason: 'the full-resync sweep must ignore the watermark',
        );
      },
    );

    test('Without a timestamp store every sweep stays a full sweep', () async {
      // No syncTimestampStorage → no watermark can be persisted → the filter can
      // never be applied, so behaviour must fall back to full sweeps.
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
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
      syncManager.dispose();

      final metaUris = metadataGetUris();
      expect(metaUris.length, greaterThanOrEqualTo(2));
      expect(metaUris.every((u) => !u.query.contains('updated_at=gt')), isTrue);
    });
  });

  // MC-413 item 6: realtime reconnect robustness. A dropped channel must be
  // resubscribed (a closed channel cannot be re-subscribed in place), and every
  // (re)connect must force a reconcile to backfill events realtime missed while
  // disconnected.
  group('Realtime reconnect', () {
    SyncManager<TestDatabase> buildManager({
      Duration inactiveAfter = const Duration(minutes: 2),
    }) {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        otherDevicesConsideredInactiveAfter: inactiveAfter,
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      return syncManager;
    }

    test('A (re)connect forces a reconcile to backfill missed events', () async {
      void Function(RealtimeSubscribeStatus, Object?)? statusCb;
      when(mockRealtimeChannel.subscribe(any)).thenAnswer((inv) {
        statusCb =
            inv.positionalArguments.first
                as void Function(RealtimeSubscribeStatus, Object?)?;
        return mockRealtimeChannel;
      });

      final syncManager = buildManager();
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      await waitForFunctionToPass(() async => expect(statusCb, isNotNull));

      // Quiesce: wait until the connect-time reconciles stop firing so the delta
      // we measure is attributable to the status callback, not a pending sync.
      var before = 0;
      await waitForFunctionToPass(() async {
        final count = syncManager.nFullSyncs;
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(syncManager.nFullSyncs, count);
        before = count;
      });

      statusCb!(RealtimeSubscribeStatus.subscribed, null);

      await waitForFunctionToPass(
        () async => expect(syncManager.nFullSyncs, greaterThan(before)),
      );
      syncManager.dispose();
    });

    test('A dropped channel is resubscribed', () async {
      var subscribeCount = 0;
      void Function(RealtimeSubscribeStatus, Object?)? statusCb;
      when(mockRealtimeChannel.subscribe(any)).thenAnswer((inv) {
        subscribeCount++;
        statusCb =
            inv.positionalArguments.first
                as void Function(RealtimeSubscribeStatus, Object?)?;
        return mockRealtimeChannel;
      });

      final syncManager = buildManager();
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();

      await waitForFunctionToPass(() async => expect(subscribeCount, 1));

      // Simulate the socket dropping under us.
      statusCb!(RealtimeSubscribeStatus.channelError, Exception('boom'));

      // A fresh channel must be built and subscribed.
      await waitForFunctionToPass(
        () async => expect(subscribeCount, greaterThanOrEqualTo(2)),
      );
      syncManager.dispose();
    });

    test('A drop is ignored once we no longer want a subscription', () async {
      var subscribeCount = 0;
      void Function(RealtimeSubscribeStatus, Object?)? statusCb;
      when(mockRealtimeChannel.subscribe(any)).thenAnswer((inv) {
        subscribeCount++;
        statusCb =
            inv.positionalArguments.first
                as void Function(RealtimeSubscribeStatus, Object?)?;
        return mockRealtimeChannel;
      });

      final syncManager = buildManager(
        inactiveAfter: const Duration(seconds: 1),
      );
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();
      await waitForFunctionToPass(() async => expect(subscribeCount, 1));

      // Other devices go inactive → the manager intentionally drops the channel.
      syncManager.setLastTimeOtherDeviceWasActive(
        DateTime.now().subtract(const Duration(seconds: 2)).toUtc(),
      );
      await waitForFunctionToPass(
        () async => expect(syncManager.isSubscribedToBackend, isFalse),
      );

      final countAfterTeardown = subscribeCount;
      // A late drop callback from the torn-down channel must not resubscribe.
      statusCb!(RealtimeSubscribeStatus.closed, null);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(subscribeCount, countAfterTeardown);

      syncManager.dispose();
    });
  });

  group('Sweep observability (MC-424 §E)', () {
    late List<LogRecord> records;
    late StreamSubscription<LogRecord> logSub;
    late Level previousRootLevel;

    setUp(() {
      previousRootLevel = Logger.root.level;
      Logger.root.level = Level.ALL;
      records = [];
      logSub = Logger.root.onRecord.listen(records.add);
    });

    tearDown(() async {
      await logSub.cancel();
      Logger.root.level = previousRootLevel;
    });

    SyncManager<TestDatabase> buildManager() {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        syncTimestampStorage: TimestampStorage(),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      return syncManager;
    }

    test('every sweep emits a per-table outcome line, even with nothing '
        'to push or pull', () async {
      // The June-10 stranded-cache incident: six consecutive sweeps fetched
      // nothing and logged nothing, leaving a multi-day outage with zero
      // telemetry. An empty sweep must still say it ran and what it saw.
      final syncManager = buildManager();
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();
      // enableSync fires its own (un-awaited) sweep; let it finish so it
      // can't bleed an outcome line into the assertion window.
      await waitForFunctionToPass(() async {
        expect(
          records.any((r) => r.message.startsWith('Sweep $itemsTable:')),
          isTrue,
        );
      });
      records.clear();

      await syncManager.syncTables();
      syncManager.dispose();

      final outcome = records.where(
        (r) =>
            r.level == Level.INFO && r.message.startsWith('Sweep $itemsTable:'),
      );
      expect(
        outcome,
        hasLength(1),
        reason: 'exactly one outcome line per table per sweep',
      );
      expect(outcome.single.message, contains('pulled 0'));
      expect(
        outcome.single.message,
        contains('watermark'),
        reason:
            'outcome must carry the watermark used for the '
            'incremental filter so an empty sweep is diagnosable',
      );
    });

    test('a failing table sweep logs a severe naming the table, then '
        'rethrows', () async {
      // Today a mid-sweep throw surfaces app-side as a bare "reconcile
      // failed" with no table attribution (FLUTTER-1R).
      final syncManager = buildManager();
      syncManager.setUserId(const Uuid().v4());
      syncManager.enableSync();
      // Let the enable-triggered sweep finish against the healthy stub
      // before poisoning the metadata fetch.
      await waitForFunctionToPass(() async {
        expect(
          records.any((r) => r.message.startsWith('Sweep $itemsTable:')),
          isTrue,
        );
      });
      when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(
        (_) async => Response(
          jsonEncode({'message': 'permission denied', 'code': '42501'}),
          403,
          request: Request('GET', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        ),
      );
      records.clear();

      await expectLater(syncManager.syncTables(), throwsA(anything));
      syncManager.dispose();

      final severe = records.where(
        (r) => r.level == Level.SEVERE && r.message.contains(itemsTable),
      );
      expect(severe, isNotEmpty, reason: 'failure must name the table');
      expect(
        severe.first.error,
        isNotNull,
        reason:
            'the cause must travel with the record so the Sentry '
            'bridge can attach it',
      );
    });

    test('a row that stays dirty across sweeps raises one stuck-row severe '
        'at the threshold, not one per sweep', () async {
      // Persistent-transient push failure: the batch re-enqueues forever and
      // dirty never clears, but (pre-§E) nothing ever reaches Sentry.
      when(
        mockHttpClient.post(
          any,
          headers: anyNamed('headers'),
          body: anyNamed('body'),
        ),
      ).thenAnswer(
        (_) async =>
            Response('server error', 500, request: Request('POST', Uri())),
      );

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final rowId = const Uuid().v4();
      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion(
              id: drift.Value(rowId),
              userId: drift.Value(userId),
              updatedAt: drift.Value(DateTime.now()),
              deleted: const drift.Value(false),
              name: const drift.Value('never pushes'),
            ),
          );

      // Twice the threshold: the detector must fire exactly once, when the
      // streak crosses the threshold — not on every later sweep.
      for (var i = 0; i < 10; i++) {
        await syncManager.syncTables();
      }
      syncManager.dispose();

      final stuck = records.where(
        (r) => r.level == Level.SEVERE && r.message.contains('Stuck row'),
      );
      expect(stuck, hasLength(1));
      expect(stuck.single.message, contains(rowId));
      expect(stuck.single.message, contains(itemsTable));
    });

    test('signed-out sweep states log at info, not warning', () async {
      // "Syncing is disabled" is the normal signed-out state. At warning it
      // becomes a Sentry capture on every signed-out launch (FLUTTER-1B/1C/1E).
      final syncManager = buildManager();
      records.clear();

      await syncManager.syncTables(); // sync never enabled
      syncManager.dispose();

      expect(
        records.where((r) => r.level >= Level.WARNING),
        isEmpty,
        reason: 'expected signed-out states must not be warnings',
      );
      expect(
        records.map((r) => r.message),
        contains(contains('syncing is disabled')),
        reason: 'the state should still be visible as a breadcrumb',
      );
    });
  });

  group('Session gate (MC-442 Phase 2)', () {
    // On cold start the local user is restored before the Supabase client
    // finishes exchanging the refresh token, so a push that wins that race goes
    // out as `anon`: auth.uid() is null, circle-scoped RLS rejects valid rows
    // with 42501, and the engine used to quarantine them for the session. The
    // engine now defends itself: it never pushes without a live session, treats
    // a session-less 42501 as transient, and clears outgoing quarantines when a
    // real auth context arrives.

    SyncManager<TestDatabase> buildManager() {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
      );
      syncManager.registerSyncable<Item>(
        backendTable: itemsTable,
        fromJson: Item.fromJson,
        companionConstructor: ItemsCompanion.new,
      );
      return syncManager;
    }

    Future<String> insertDirtyRow(String userId, {String? name}) async {
      final id = const Uuid().v4();
      await testDb
          .into(testDb.items)
          .insert(
            ItemsCompanion(
              id: drift.Value(id),
              userId: drift.Value(userId),
              updatedAt: drift.Value(DateTime.now()),
              deleted: const drift.Value(false),
              name: drift.Value(name ?? 'Gated'),
            ),
          );
      return id;
    }

    test('a dirty row is NOT pushed while there is no live session — it is '
        'withheld, not quarantined', () async {
      // No session: every push would land as anon and get RLS-rejected.
      when(mockGoTrue.currentSession).thenReturn(null);

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final id = await insertDirtyRow(userId);

      // Give the loop ample passes to (wrongly) push if the gate were missing.
      await Future.delayed(const Duration(milliseconds: 150));

      verifyNever(
        mockQueryBuilder.upsert(any, onConflict: anyNamed('onConflict')),
      );
      final row = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(id))).getSingle();
      expect(
        row.dirty,
        isTrue,
        reason: 'row must stay dirty (withheld), never dropped',
      );
      expect(syncManager.nSyncedToBackend(Item), 0);

      syncManager.dispose();
    });

    test('an expired session counts as no session — the push is withheld',
        () async {
      // currentSession is non-null but expired: the access token can no longer
      // authorize a write, so it is as good as anon.
      when(mockSession.isExpired).thenReturn(true);

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      await insertDirtyRow(userId);
      await Future.delayed(const Duration(milliseconds: 150));

      verifyNever(
        mockQueryBuilder.upsert(any, onConflict: anyNamed('onConflict')),
      );
      expect(syncManager.nSyncedToBackend(Item), 0);

      syncManager.dispose();
    });

    test('rows withheld while session-less flush as soon as a session goes '
        'live (auth-event wake)', () async {
      when(mockGoTrue.currentSession).thenReturn(null);

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final id = await insertDirtyRow(userId);

      // Withheld so far.
      await Future.delayed(const Duration(milliseconds: 80));
      final before = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(id))).getSingle();
      expect(before.dirty, isTrue, reason: 'still withheld, no session yet');

      // The refresh-token exchange completes: a live session lands and the
      // client fires signedIn. The withheld row must now flush.
      when(mockGoTrue.currentSession).thenReturn(mockSession);
      authEvents.add(AuthState(AuthChangeEvent.signedIn, mockSession));

      await waitForFunctionToPass(() async {
        final row = await (testDb.select(
          testDb.items,
        )..where((t) => t.id.equals(id))).getSingle();
        expect(row.dirty, isFalse, reason: 'row flushed once session was live');
      });

      syncManager.dispose();
    });

    test('a 42501 reached after the session drops mid-push is retried, never '
        'quarantined', () async {
      // The gate passes (session live), the push goes out, but the session is
      // gone by the time the verdict lands — so this 42501 was decided without a
      // live auth context and says nothing about the row. It must be retried
      // when a session returns, NOT quarantined for the session. The backend
      // keeps rejecting with 42501 while there is no session (every attempt,
      // batch or per-row fallback), and only accepts once a session is back —
      // so a quarantine at v1 would strand the row forever (no auth event here
      // to clear it), while a retry flushes it.
      var sessionRestored = false;
      var rejections = 0;
      when(
        mockHttpClient.post(
          any,
          headers: anyNamed('headers'),
          body: anyNamed('body'),
        ),
      ).thenAnswer((inv) async {
        final body = inv.namedArguments[#body] as String;
        final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
        if (!sessionRestored) {
          // Drop the session mid-push: the verdict below is reached as anon.
          when(mockGoTrue.currentSession).thenReturn(null);
          rejections++;
          return Response(
            jsonEncode({
              'code': '42501',
              'message': 'new row violates row-level security policy',
              'details': null,
              'hint': null,
            }),
            403,
            request: Request('POST', Uri()),
            headers: {'content-type': 'application/json; charset=utf-8'},
          );
        }
        return Response(
          jsonEncode(rows),
          200,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      });

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final id = await insertDirtyRow(userId);

      // At least one session-less 42501 was reached, and the row is still dirty.
      await waitForFunctionToPass(() async {
        expect(rejections, greaterThanOrEqualTo(1));
      });
      final stillDirty = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(id))).getSingle();
      expect(stillDirty.dirty, isTrue, reason: 'not yet flushed (no session)');

      // Manual recovery restores the session WITHOUT a fresh auth event, so the
      // only thing that can flush the row is change #2 (retry, not quarantine).
      // If the engine had quarantined it at v1, it would never flush here.
      sessionRestored = true;
      when(mockGoTrue.currentSession).thenReturn(mockSession);

      await waitForFunctionToPass(() async {
        final row = await (testDb.select(
          testDb.items,
        )..where((t) => t.id.equals(id))).getSingle();
        expect(
          row.dirty,
          isFalse,
          reason: 'session-less 42501 was retried, not quarantined',
        );
      });
      expect(
        rejections,
        greaterThanOrEqualTo(1),
        reason: 'the row really was rejected while session-less, then retried',
      );

      syncManager.dispose();
    });

    test('a signedIn event clears the outgoing quarantine so a row rejected '
        'without real auth gets another chance', () async {
      // A row is quarantined while the session reads live (a genuine-looking
      // 42501). A verdict reached before a real auth context says nothing about
      // the row, so a fresh signedIn must clear the quarantine and let it retry;
      // here the backend accepts it once auth has "arrived", proving the
      // quarantine was cleared (a still-versioned quarantine would never flush).
      var authArrived = false;
      when(
        mockHttpClient.post(
          any,
          headers: anyNamed('headers'),
          body: anyNamed('body'),
        ),
      ).thenAnswer((inv) async {
        final body = inv.namedArguments[#body] as String;
        final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
        if (!authArrived) {
          return Response(
            jsonEncode({
              'code': '42501',
              'message': 'new row violates row-level security policy',
              'details': null,
              'hint': null,
            }),
            403,
            request: Request('POST', Uri()),
            headers: {'content-type': 'application/json; charset=utf-8'},
          );
        }
        return Response(
          jsonEncode(rows),
          200,
          request: Request('POST', Uri()),
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      });

      final syncManager = buildManager();
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final id = await insertDirtyRow(userId);

      // It gets pushed, rejected (42501), and quarantined — it must NOT keep
      // retrying at the same version, so it stays dirty.
      await Future.delayed(const Duration(milliseconds: 150));
      final quarantined = await (testDb.select(
        testDb.items,
      )..where((t) => t.id.equals(id))).getSingle();
      expect(quarantined.dirty, isTrue, reason: 'row is quarantined, still dirty');

      // A real auth context arrives. Clearing the quarantine lets the row retry,
      // and the backend now accepts it.
      authArrived = true;
      authEvents.add(AuthState(AuthChangeEvent.signedIn, mockSession));

      await waitForFunctionToPass(() async {
        final row = await (testDb.select(
          testDb.items,
        )..where((t) => t.id.equals(id))).getSingle();
        expect(
          row.dirty,
          isFalse,
          reason: 'quarantine cleared on signedIn; row flushed',
        );
      });

      syncManager.dispose();
    });
  });
}
