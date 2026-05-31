import 'dart:convert';

import 'package:drift/drift.dart' as drift;
import 'package:drift/native.dart' as drift_native;
import 'package:http/http.dart';
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

void main() {
  late TestDatabase testDb;

  late MockSupabaseClient mockSupabaseClient;
  late MockSupabaseQueryBuilder mockQueryBuilder;
  late MockClient mockHttpClient;

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
    final mockRealtimeChannel = MockRealtimeChannel();
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
      mockRealtimeChannel.subscribe(),
    ).thenAnswer((_) => mockRealtimeChannel);
  });

  tearDown(() async {
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
}
