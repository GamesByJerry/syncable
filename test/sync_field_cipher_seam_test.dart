import 'dart:async';
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

import 'utils/fake_field_cipher.dart';
import 'utils/test_database.dart';
import 'utils/test_mocks.mocks.dart';
import 'utils/test_supabase_names.dart';
import 'utils/wait_for_function_to_pass.dart';

class _MemoryTimestampStorage extends SyncTimestampStorage {
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

/// MC-427: the field-cipher codec seam at the Syncable push/pull boundary.
///
/// Content fields of registered tables are folded into one AEAD blob per row
/// (`content_enc` + `key_version`) at push time and merged back before
/// `fromJson` at pull time. The local database stays plaintext throughout —
/// only the wire representation changes.
void main() {
  late TestDatabase testDb;

  late MockSupabaseClient mockSupabaseClient;
  late MockSupabaseQueryBuilder mockQueryBuilder;
  late MockClient mockHttpClient;
  late MockRealtimeChannel mockRealtimeChannel;
  late MockGoTrueClient mockGoTrue;
  late MockSession mockSession;
  late StreamController<AuthState> authEvents;

  /// Bodies of every upsert POST that reached the (mock) backend, in order.
  late List<List<Map<String, dynamic>>> pushedBatches;

  /// Wire rows the (mock) backend serves to metadata sweeps and batch pulls.
  late List<Map<String, dynamic>> backendRows;

  /// All rows ever pushed, flattened.
  List<Map<String, dynamic>> pushedRows() => [
    for (final batch in pushedBatches) ...batch,
  ];

  setUp(() {
    testDb = TestDatabase(
      drift.DatabaseConnection(
        drift_native.NativeDatabase.memory(),
        closeStreamsSynchronously: true,
      ),
    );

    pushedBatches = [];
    backendRows = [];

    mockSupabaseClient = MockSupabaseClient();
    mockQueryBuilder = MockSupabaseQueryBuilder();
    mockHttpClient = MockClient();

    // A live session by default so the engine's session gate (MC-442) lets
    // pushes through; these tests are about the cipher seam, not auth.
    mockGoTrue = MockGoTrueClient();
    mockSession = MockSession();
    authEvents = StreamController<AuthState>.broadcast();
    when(mockSupabaseClient.auth).thenReturn(mockGoTrue);
    when(mockGoTrue.currentSession).thenReturn(mockSession);
    // Session is deliberately live in this suite. Mockito's generated fallback
    // for an unstubbed bool getter is false today, but make the contract explicit
    // so auth-gate behavior cannot silently change with mock generation.
    when(mockSession.isExpired).thenReturn(false);
    when(mockGoTrue.onAuthStateChange).thenAnswer((_) => authEvents.stream);

    final realQueryBuilder = PostgrestQueryBuilder(
      url: Uri(),
      httpClient: mockHttpClient,
    );

    when(
      mockSupabaseClient.from(secretItemsTable),
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
    ).thenAnswer((inv) async {
      final body = inv.namedArguments[#body] as String;
      pushedBatches.add(
        (jsonDecode(body) as List).cast<Map<String, dynamic>>(),
      );
      return Response(
        body,
        200,
        request: Request('POST', Uri()),
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });
    when(mockQueryBuilder.select(any)).thenAnswer(
      (inv) => realQueryBuilder.select(inv.positionalArguments[0] as String),
    );
    // Current PostgREST executes requests through BaseClient.send(). Preserve
    // the suite's original semantics: GETs read [backendRows], POSTs record the
    // exact upsert payload and echo it back as a successful response.
    when(mockHttpClient.send(any)).thenAnswer((invocation) async {
      final request = invocation.positionalArguments[0] as BaseRequest;
      if (request.method == 'POST') {
        final body = (request as Request).body;
        pushedBatches.add(
          (jsonDecode(body) as List).cast<Map<String, dynamic>>(),
        );
        return StreamedResponse(
          Stream<List<int>>.value(utf8.encode(body)),
          200,
          request: request,
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }

      final bytes = utf8.encode(jsonEncode(backendRows));
      return StreamedResponse(
        Stream<List<int>>.value(bytes),
        200,
        request: request,
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

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

  final circleId = const Uuid().v4();

  SyncEncryption secretItemsEncryption() => SyncEncryption(
    encryptedFields: const {titleKey, amountKey},
    lockedFieldPlaceholders: const {titleKey: '🔒', amountKey: 0},
  );

  SyncManager<TestDatabase> buildManager({
    required FakeFieldCipher? cipher,
    List<SyncEncryptionAlert>? alerts,
    SyncEncryption? encryption,
  }) {
    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
      fieldCipher: cipher,
      onEncryptionAlert: alerts?.add,
    );
    syncManager.registerSyncable<SecretItem>(
      backendTable: secretItemsTable,
      fromJson: SecretItem.fromJson,
      companionConstructor: SecretItemsCompanion.new,
      encryption: encryption,
    );
    return syncManager;
  }

  Future<SecretItem> insertLocal({
    required String userId,
    String? id,
    String? circle,
    String title = 'top secret',
    int amount = 42,
    String? assignee,
    DateTime? updatedAt,
    bool deleted = false,
  }) async {
    return await testDb
        .into(testDb.secretItems)
        .insertReturning(
          SecretItemsCompanion(
            id: id == null ? const drift.Value.absent() : drift.Value(id),
            userId: drift.Value(userId),
            updatedAt: drift.Value(updatedAt ?? DateTime.now().toUtc()),
            deleted: drift.Value(deleted),
            circleId: drift.Value(circle ?? circleId),
            title: drift.Value(title),
            amount: drift.Value(amount),
            assignee: drift.Value(assignee),
          ),
        );
  }

  /// A backend wire row as an `enforced` push would have produced it: content
  /// fields nulled, blob attached.
  Map<String, dynamic> enforcedWireRow(
    FakeFieldCipher cipher, {
    required String id,
    required String userId,
    required DateTime updatedAt,
    String? circle,
    String title = 'from backend',
    int amount = 7,
    String? assignee,
    int? keyVersion,
    String? contentEnc,
    bool deleted = false,
  }) {
    final version = keyVersion ?? cipher.activeKeyVersion;
    return {
      idKey: id,
      userIdKey: userId,
      updatedAtKey: updatedAt.toIso8601String(),
      deletedKey: deleted,
      circleIdKey: circle ?? circleId,
      titleKey: null,
      amountKey: null,
      assigneeKey: assignee,
      contentEncKey:
          contentEnc ??
          cipher.blobFor(
            table: secretItemsTable,
            rowId: id,
            circleId: circle ?? circleId,
            keyVersion: version,
            fields: {titleKey: title, amountKey: amount},
          ),
      keyVersionKey: version,
    };
  }

  Future<SecretItem> localRow(String id) async {
    return await (testDb.select(
      testDb.secretItems,
    )..where((t) => t.id.equals(id))).getSingle();
  }

  /// Reproduces the same-row realtime decode race (Codex P2 on PR #8):
  ///
  /// Two realtime events for one row arrive in backend order (old, new), but
  /// the OLD one decrypts slower, so it finishes decoding — and enqueues —
  /// last. Both must land in the same drain batch for the queue collapse to
  /// be the deciding step, so the sync loop's pass is kept busy pushing a
  /// dirty row of a second registered table ([Items]) whose mock upsert
  /// responds slowly. Timeline (ms): 0 events fired → ~20 new enqueued →
  /// ~60 old enqueued → ~250 items push returns, loop drains both together.
  ///
  /// With [withKey] the versions decrypt (content collapse case); without it
  /// both arrive undecryptable under key versions 5/6 (locked-fallback
  /// alignment case).
  Future<
    ({
      SyncManager<TestDatabase> syncManager,
      String rowId,
      DateTime newUpdatedAt,
      Map<String, dynamic> newWire,
    })
  >
  runOutOfOrderRealtimeScenario({required bool withKey}) async {
    final pgCallbacks = <String, void Function(PostgresChangePayload)>{};
    when(
      mockRealtimeChannel.onPostgresChanges(
        schema: anyNamed('schema'),
        table: anyNamed('table'),
        event: anyNamed('event'),
        callback: anyNamed('callback'),
      ),
    ).thenAnswer((inv) {
      pgCallbacks[inv.namedArguments[#table] as String] =
          inv.namedArguments[#callback] as void Function(PostgresChangePayload);
      return mockRealtimeChannel;
    });

    // The items table gets its own (slow) upsert path; a POST is recognized
    // as an items push by the presence of the `name` column.
    final itemsQueryBuilder = MockSupabaseQueryBuilder();
    final realQueryBuilder = PostgrestQueryBuilder(
      url: Uri(),
      httpClient: mockHttpClient,
    );
    when(
      mockSupabaseClient.from(itemsTable),
    ).thenAnswer((_) => itemsQueryBuilder);
    when(
      itemsQueryBuilder.upsert(any, onConflict: anyNamed('onConflict')),
    ).thenAnswer(
      (inv) => realQueryBuilder.upsert(
        inv.positionalArguments[0] as Object,
        onConflict: inv.namedArguments[#onConflict] as String?,
      ),
    );
    when(itemsQueryBuilder.select(any)).thenAnswer(
      (inv) => realQueryBuilder.select(inv.positionalArguments[0] as String),
    );
    var itemsPushStarted = false;
    when(mockHttpClient.send(any)).thenAnswer((invocation) async {
      final request = invocation.positionalArguments[0] as BaseRequest;
      if (request.method == 'POST') {
        final body = (request as Request).body;
        final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();
        if (rows.isNotEmpty && rows.first.containsKey(nameKey)) {
          itemsPushStarted = true;
          await Future<void>.delayed(const Duration(milliseconds: 250));
        } else {
          pushedBatches.add(rows);
        }
        return StreamedResponse(
          Stream<List<int>>.value(utf8.encode(body)),
          200,
          request: request,
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      }

      return StreamedResponse(
        Stream<List<int>>.value(utf8.encode(jsonEncode(backendRows))),
        200,
        request: request,
        headers: {'content-type': 'application/json; charset=utf-8'},
      );
    });

    final cipher = FakeFieldCipher()
      ..circleModes[circleId] = SyncEncryptionMode.enforced;
    if (withKey) cipher.keys[circleId] = {1};

    final syncManager = SyncManager<TestDatabase>(
      localDatabase: testDb,
      supabaseClient: mockSupabaseClient,
      syncInterval: const Duration(milliseconds: 1),
      fieldCipher: cipher,
    );
    // Items first: each loop pass pushes Items before draining SecretItems'
    // incoming queue, so the slow items POST holds the batch window open.
    syncManager.registerSyncable<Item>(
      backendTable: itemsTable,
      fromJson: Item.fromJson,
      companionConstructor: ItemsCompanion.new,
    );
    syncManager.registerSyncable<SecretItem>(
      backendTable: secretItemsTable,
      fromJson: SecretItem.fromJson,
      companionConstructor: SecretItemsCompanion.new,
      encryption: secretItemsEncryption(),
    );

    final userId = const Uuid().v4();
    syncManager.setUserId(userId);
    syncManager.enableSync();
    await waitForFunctionToPass(
      () async => expect(pgCallbacks, contains(secretItemsTable)),
    );

    final rowId = const Uuid().v4();
    final oldUpdatedAt = DateTime.now().toUtc();
    final newUpdatedAt = oldUpdatedAt.add(const Duration(seconds: 1));
    final oldWire = enforcedWireRow(
      cipher,
      id: rowId,
      userId: userId,
      updatedAt: oldUpdatedAt,
      title: 'old version',
      amount: 1,
      keyVersion: withKey ? 1 : 5,
    );
    final newWire = enforcedWireRow(
      cipher,
      id: rowId,
      userId: userId,
      updatedAt: newUpdatedAt,
      title: 'new version',
      amount: 2,
      keyVersion: withKey ? 1 : 6,
    );
    // The OLDER event decrypts slower than the newer one.
    cipher.blobDecryptDelays[oldWire[contentEncKey] as String] = const Duration(
      milliseconds: 60,
    );
    cipher.blobDecryptDelays[newWire[contentEncKey] as String] = const Duration(
      milliseconds: 20,
    );

    // Occupy the loop pass with a slow items push...
    await testDb
        .into(testDb.items)
        .insert(
          ItemsCompanion(
            userId: drift.Value(userId),
            updatedAt: drift.Value(DateTime.now().toUtc()),
            name: const drift.Value('keeps the loop busy'),
          ),
        );
    await waitForFunctionToPass(() async => expect(itemsPushStarted, isTrue));

    // ...and deliver both events, in backend order, while it is blocked.
    PostgresChangePayload payload(Map<String, dynamic> wire) =>
        PostgresChangePayload(
          schema: 'public',
          table: secretItemsTable,
          commitTimestamp: DateTime.now(),
          eventType: PostgresChangeEvent.update,
          newRecord: wire,
          oldRecord: const {},
          errors: null,
        );
    pgCallbacks[secretItemsTable]!(payload(oldWire));
    pgCallbacks[secretItemsTable]!(payload(newWire));

    return (
      syncManager: syncManager,
      rowId: rowId,
      newUpdatedAt: newUpdatedAt,
      newWire: newWire,
    );
  }

  group('Push', () {
    test(
      'enforced: content fields are nulled and the blob is attached; '
      'plaintext sync columns and unregistered fields are untouched',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced
          ..keys[circleId] = {1};
        final alerts = <SyncEncryptionAlert>[];
        final syncManager = buildManager(
          cipher: cipher,
          alerts: alerts,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.enableSync();
        syncManager.setUserId(userId);

        final assignee = const Uuid().v4();
        final item = await insertLocal(userId: userId, assignee: assignee);

        await waitForFunctionToPass(() async {
          expect(syncManager.nSyncedToBackend(SecretItem), 1);
        });

        final row = pushedRows().single;
        expect(row[titleKey], isNull);
        expect(row[amountKey], isNull);
        expect(row[contentEncKey], isNotNull);
        expect(row[keyVersionKey], 1);
        // Plaintext envelope intact: identity, LWW timestamp, tombstone, RLS
        // scope, and the FK-style plaintext content column.
        expect(row[idKey], item.id);
        expect(row[userIdKey], userId);
        expect(row[updatedAtKey], item.updatedAt.toIso8601String());
        expect(row[deletedKey], false);
        expect(row[circleIdKey], circleId);
        expect(row[assigneeKey], assignee);
        // The local cache stays plaintext — only the wire is encrypted.
        expect((await localRow(item.id)).title, 'top secret');
        expect(alerts, isEmpty);

        syncManager.dispose();
      },
    );

    test('shadow: pushes the plaintext fields AND the blob', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.shadow
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      await insertLocal(userId: userId);

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });

      final row = pushedRows().single;
      expect(row[titleKey], 'top secret');
      expect(row[amountKey], 42);
      expect(row[contentEncKey], isNotNull);
      expect(row[keyVersionKey], 1);

      syncManager.dispose();
    });

    test(
      'off (registered table): plaintext passes through with explicit '
      'content_enc/key_version nulls so a restored circle sheds stale blobs',
      () async {
        final cipher = FakeFieldCipher(); // defaultMode: off
        final syncManager = buildManager(
          cipher: cipher,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.enableSync();
        syncManager.setUserId(userId);

        await insertLocal(userId: userId);

        await waitForFunctionToPass(() async {
          expect(syncManager.nSyncedToBackend(SecretItem), 1);
        });

        final row = pushedRows().single;
        expect(row[titleKey], 'top secret');
        expect(row[amountKey], 42);
        expect(row.containsKey(contentEncKey), isTrue);
        expect(row[contentEncKey], isNull);
        expect(row[keyVersionKey], isNull);
        expect(cipher.encryptCalls, isEmpty);

        syncManager.dispose();
      },
    );

    test('a table registered WITHOUT encryption never consults the cipher and '
        'its payload carries no encryption keys at all', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(cipher: cipher);
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      await insertLocal(userId: userId);

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });

      final row = pushedRows().single;
      expect(row[titleKey], 'top secret');
      expect(row.containsKey(contentEncKey), isFalse);
      expect(row.containsKey(keyVersionKey), isFalse);
      expect(cipher.encryptCalls, isEmpty);
      expect(cipher.decryptCalls, isEmpty);

      syncManager.dispose();
    });

    test('enforced with a missing key: the row is withheld (no plaintext ever '
        'leaves), an info alert fires, and the row pushes after the key '
        'arrives via retryLockedRows()', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced; // no keys!
      final alerts = <SyncEncryptionAlert>[];
      final syncManager = buildManager(
        cipher: cipher,
        alerts: alerts,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.enableSync();
      syncManager.setUserId(userId);

      final item = await insertLocal(userId: userId);

      await waitForFunctionToPass(() async {
        expect(
          alerts.where((a) => a.kind == SyncEncryptionAlertKind.keyUnavailable),
          isNotEmpty,
        );
      });
      // Settle a few loop passes, then verify nothing was pushed and the
      // alert did not re-fire every pass (deferred rows are skipped, the
      // queue is not spinning).
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(pushedRows(), isEmpty);
      expect(syncManager.nSyncedToBackend(SecretItem), 0);
      expect(
        alerts
            .where((a) => a.kind == SyncEncryptionAlertKind.keyUnavailable)
            .length,
        1,
      );
      expect(alerts.single.severity, SyncEncryptionAlertSeverity.info);
      expect((await localRow(item.id)).dirty, isTrue);

      // The key lands (MC-428 will deliver it) — retry unblocks the push.
      cipher.keys[circleId] = {1};
      await syncManager.retryLockedRows();

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });
      final row = pushedRows().single;
      expect(row[titleKey], isNull);
      expect(row[contentEncKey], isNotNull);
      expect((await localRow(item.id)).dirty, isFalse);

      syncManager.dispose();
    });

    test('a deferred push survives a restart even when the persisted push '
        'watermark moved past it: retryLockedRows re-enqueues from the '
        'database, not the in-memory deferral map', () async {
      // The failure mode (Codex review on PR #8): row A is deferred for a
      // missing key; row B pushes successfully afterwards, advancing the
      // PERSISTED last-pushed watermark beyond A's updatedAt. After a
      // restart the deferral map is gone and the local-change sweep filters
      // A out (updatedAt <= watermark), so without DB-driven recovery the
      // row would stay dirty-but-unpushed forever.
      final timestamps = _MemoryTimestampStorage();
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced; // no key yet
      final alerts = <SyncEncryptionAlert>[];

      SyncManager<TestDatabase> build() {
        final manager = SyncManager<TestDatabase>(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(milliseconds: 1),
          syncTimestampStorage: timestamps,
          fieldCipher: cipher,
          onEncryptionAlert: alerts.add,
        );
        manager.registerSyncable<SecretItem>(
          backendTable: secretItemsTable,
          fromJson: SecretItem.fromJson,
          companionConstructor: SecretItemsCompanion.new,
          encryption: secretItemsEncryption(),
        );
        return manager;
      }

      final userId = const Uuid().v4();
      final firstRun = build();
      firstRun.enableSync();
      firstRun.setUserId(userId);

      // Row A: enforced circle without a key — push deferred.
      final itemA = await insertLocal(
        userId: userId,
        title: 'deferred secret',
        updatedAt: DateTime.now().toUtc(),
      );
      await waitForFunctionToPass(() async {
        expect(
          alerts.where((a) => a.kind == SyncEncryptionAlertKind.keyUnavailable),
          isNotEmpty,
        );
      });

      // Row B: a circle in off mode — pushes fine and advances the
      // persisted watermark past row A's updatedAt.
      final otherCircle = const Uuid().v4();
      await insertLocal(
        userId: userId,
        circle: otherCircle,
        title: 'plain row',
        updatedAt: DateTime.now().toUtc().add(const Duration(seconds: 1)),
      );
      await waitForFunctionToPass(() async {
        expect(firstRun.nSyncedToBackend(SecretItem), 1);
      });

      // "Restart": fresh manager, same database + persisted timestamps.
      firstRun.dispose();
      cipher.keys[circleId] = {1}; // the key arrived while we were away
      final secondRun = build();
      secondRun.enableSync();
      secondRun.setUserId(userId);

      // The app's key bootstrap calls this after sign-in / key arrival.
      await secondRun.retryLockedRows();

      await waitForFunctionToPass(() async {
        final row = await localRow(itemA.id);
        expect(row.dirty, isFalse, reason: 'deferred row finally pushed');
      });
      final pushedA = pushedRows().where((r) => r[idKey] == itemA.id).toList();
      expect(pushedA, isNotEmpty);
      expect(pushedA.last[contentEncKey], isNotNull);
      expect(pushedA.last[titleKey], isNull);

      secondRun.dispose();
    });
  });

  group('Pull', () {
    test(
      'enforced wire row decrypts on batch pull: the local row is plaintext, '
      'clean, and unlocked',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced
          ..keys[circleId] = {1};
        final syncManager = buildManager(
          cipher: cipher,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        final id = const Uuid().v4();
        backendRows = [
          enforcedWireRow(
            cipher,
            id: id,
            userId: userId,
            updatedAt: DateTime.now().toUtc(),
            title: 'decrypted title',
            amount: 99,
            assignee: userId,
          ),
        ];

        await waitForFunctionToPass(() async {
          await syncManager.syncTables();
          final row = await localRow(id);
          expect(row.title, 'decrypted title');
          expect(row.amount, 99);
          expect(row.assignee, userId);
          expect(row.dirty, isFalse);
          expect(row.locked, isFalse);
          expect(row.lockedContentEnc, isNull);
        });

        syncManager.dispose();
      },
    );

    test(
      'a realtime event with an encrypted row decrypts on receipt',
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

        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced
          ..keys[circleId] = {1};
        final syncManager = buildManager(
          cipher: cipher,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        await waitForFunctionToPass(() async => expect(pgCallback, isNotNull));

        final id = const Uuid().v4();
        pgCallback!(
          PostgresChangePayload(
            schema: 'public',
            table: secretItemsTable,
            commitTimestamp: DateTime.now(),
            eventType: PostgresChangeEvent.insert,
            newRecord: enforcedWireRow(
              cipher,
              id: id,
              userId: userId,
              updatedAt: DateTime.now().toUtc(),
              title: 'realtime secret',
              amount: 5,
            ),
            oldRecord: const {},
            errors: null,
          ),
        );

        await waitForFunctionToPass(() async {
          final row = await localRow(id);
          expect(row.title, 'realtime secret');
          expect(row.amount, 5);
          expect(row.locked, isFalse);
          expect(row.dirty, isFalse);
        });

        syncManager.dispose();
      },
    );

    test('realtime events for the same row that decrypt out of order never let '
        'an older version shadow a newer one', () async {
      // Codex P2 on PR #8: decode is fire-and-forget, so two events for the
      // same id can enqueue in decrypt-COMPLETION order. When both land in
      // the same drain batch (here: the loop pass is busy pushing another
      // table), the queue collapse must keep the newest updatedAt, not the
      // last-enqueued entry.
      final scenario = await runOutOfOrderRealtimeScenario(withKey: true);

      await waitForFunctionToPass(() async {
        final row = await localRow(scenario.rowId);
        expect(row.title, 'new version');
        expect(row.amount, 2);
        expect(row.updatedAt, scenario.newUpdatedAt);
        expect(row.locked, isFalse);
      });

      scenario.syncManager.dispose();
    });

    test('out-of-order LOCKED decodes keep the preserved ciphertext aligned '
        'with the newest version (no blob ever dropped)', () async {
      // Same race as above, but neither version is decryptable: the locked
      // fallback instruction must stay version-aligned with the surviving
      // row — a mismatched instruction must never strip a locked row of
      // its ciphertext.
      final scenario = await runOutOfOrderRealtimeScenario(withKey: false);

      await waitForFunctionToPass(() async {
        final row = await localRow(scenario.rowId);
        expect(row.updatedAt, scenario.newUpdatedAt);
        expect(row.locked, isTrue);
        expect(row.lockedContentEnc, scenario.newWire[contentEncKey]);
        expect(row.lockedKeyVersion, 6);
      });

      scenario.syncManager.dispose();
    });

    test(
      'round-trip: an enforced push fed back through pull restores the exact '
      'content and updated_at (LWW timestamp survives encryption)',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced
          ..keys[circleId] = {1};
        final syncManager = buildManager(
          cipher: cipher,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.enableSync();
        syncManager.setUserId(userId);

        final item = await insertLocal(
          userId: userId,
          title: 'round trip',
          amount: 123,
        );
        await waitForFunctionToPass(() async {
          expect(syncManager.nSyncedToBackend(SecretItem), 1);
        });
        final pushed = pushedRows().single;
        syncManager.dispose();

        // A second device pulls exactly what the first one pushed.
        await testDb.clear();
        final secondManager = buildManager(
          cipher: cipher,
          encryption: secretItemsEncryption(),
        );
        secondManager.setUserId(userId);
        secondManager.enableSync();
        backendRows = [pushed];

        await waitForFunctionToPass(() async {
          await secondManager.syncTables();
          final row = await localRow(item.id);
          expect(row.title, 'round trip');
          expect(row.amount, 123);
          expect(row.updatedAt, item.updatedAt);
          expect(row.locked, isFalse);
        });

        secondManager.dispose();
      },
    );

    test('a missing key locks the row (placeholders + preserved ciphertext + '
        'info alert); retryLockedRows() recovers the content locally once the '
        'key arrives — no network, no re-push', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced; // no keys
      final alerts = <SyncEncryptionAlert>[];
      final syncManager = buildManager(
        cipher: cipher,
        alerts: alerts,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      final wire = enforcedWireRow(
        cipher,
        id: id,
        userId: userId,
        updatedAt: DateTime.now().toUtc(),
        title: 'locked away',
        amount: 11,
        keyVersion: 3,
      );
      backendRows = [wire];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.locked, isTrue);
        expect(row.title, '🔒');
        expect(row.amount, 0);
        expect(row.lockedContentEnc, wire[contentEncKey]);
        expect(row.lockedKeyVersion, 3);
        expect(row.dirty, isFalse);
      });
      expect(alerts, isNotEmpty);
      expect(alerts.first.kind, SyncEncryptionAlertKind.keyUnavailable);
      expect(alerts.first.severity, SyncEncryptionAlertSeverity.info);
      expect(alerts.first.keyVersion, 3);

      // Key v3 arrives — recovery is local-only.
      backendRows = [];
      cipher.keys[circleId] = {3};
      final unlocked = await syncManager.retryLockedRows();
      expect(unlocked, 1);

      final row = await localRow(id);
      expect(row.locked, isFalse);
      expect(row.title, 'locked away');
      expect(row.amount, 11);
      expect(row.lockedContentEnc, isNull);
      expect(row.lockedKeyVersion, isNull);
      expect(row.dirty, isFalse, reason: 'unlock is not a local edit');

      // The unlock never causes a push.
      await syncManager.syncTables();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(syncManager.nSyncedToBackend(SecretItem), 0);

      syncManager.dispose();
    });

    test(
      'a transplanted blob (valid ciphertext under the wrong row binding) '
      'locks the row and fires a CRITICAL possible-tampering alert',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced
          ..keys[circleId] = {1};
        final alerts = <SyncEncryptionAlert>[];
        final syncManager = buildManager(
          cipher: cipher,
          alerts: alerts,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        // A malicious server transplants row A's (valid!) blob onto row B.
        final rowA = const Uuid().v4();
        final rowB = const Uuid().v4();
        final blobForA = cipher.blobFor(
          table: secretItemsTable,
          rowId: rowA,
          circleId: circleId,
          keyVersion: 1,
          fields: const {titleKey: 'someone elses secret', amountKey: 1},
        );
        backendRows = [
          enforcedWireRow(
            cipher,
            id: rowB,
            userId: userId,
            updatedAt: DateTime.now().toUtc(),
            contentEnc: blobForA,
            keyVersion: 1,
          ),
        ];

        await waitForFunctionToPass(() async {
          await syncManager.syncTables();
          final row = await localRow(rowB);
          // Decryption fails loudly; the swapped content is NEVER rendered.
          expect(row.locked, isTrue);
          expect(row.title, '🔒');
          expect(row.lockedContentEnc, blobForA);
        });
        expect(
          alerts.map((a) => a.kind),
          contains(SyncEncryptionAlertKind.possibleTampering),
        );
        expect(
          alerts
              .firstWhere(
                (a) => a.kind == SyncEncryptionAlertKind.possibleTampering,
              )
              .severity,
          SyncEncryptionAlertSeverity.critical,
        );

        syncManager.dispose();
      },
    );

    test('a legacy plaintext row in a registered table passes through '
        '(mixed-state read path during migration)', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      backendRows = [
        {
          idKey: id,
          userIdKey: userId,
          updatedAtKey: DateTime.now().toUtc().toIso8601String(),
          deletedKey: false,
          circleIdKey: circleId,
          titleKey: 'legacy plaintext',
          amountKey: 1,
          assigneeKey: null,
          contentEncKey: null,
          keyVersionKey: null,
        },
      ];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.title, 'legacy plaintext');
        expect(row.locked, isFalse);
      });
      expect(cipher.decryptCalls, isEmpty);

      syncManager.dispose();
    });

    test('unknown fields inside the blob are tolerated (a newer client wrote '
        'them — blob self-versioning, forward compatibility)', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: DateTime.now().toUtc(),
          contentEnc: cipher.blobFor(
            table: secretItemsTable,
            rowId: id,
            circleId: circleId,
            keyVersion: 1,
            fields: const {
              titleKey: 'future row',
              amountKey: 8,
              'field_from_the_future': 'ignored',
            },
          ),
          keyVersion: 1,
        ),
      ];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.title, 'future row');
        expect(row.amount, 8);
      });

      syncManager.dispose();
    });

    test('a blob with no key_version cannot select a key: treated as key '
        'unavailability (locked + info), not tampering', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final alerts = <SyncEncryptionAlert>[];
      final syncManager = buildManager(
        cipher: cipher,
        alerts: alerts,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      final wire = enforcedWireRow(
        cipher,
        id: id,
        userId: userId,
        updatedAt: DateTime.now().toUtc(),
      );
      wire[keyVersionKey] = null;
      backendRows = [wire];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.locked, isTrue);
        expect(row.lockedContentEnc, wire[contentEncKey]);
        expect(row.lockedKeyVersion, isNull);
      });
      expect(alerts.first.kind, SyncEncryptionAlertKind.keyUnavailable);
      expect(alerts.first.severity, SyncEncryptionAlertSeverity.info);

      syncManager.dispose();
    });

    test('a blob cannot override the plaintext envelope: protected keys inside '
        'the decrypted fields are discarded (defense against a crafted blob '
        'manipulating LWW or row identity)', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      final wireUpdatedAt = DateTime.now().toUtc();
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: wireUpdatedAt,
          contentEnc: cipher.blobFor(
            table: secretItemsTable,
            rowId: id,
            circleId: circleId,
            keyVersion: 1,
            fields: {
              titleKey: 'sneaky',
              amountKey: 1,
              // A circle member with the key crafts a blob trying to spoof
              // the plaintext envelope of this row.
              updatedAtKey: DateTime.now()
                  .add(const Duration(days: 3650))
                  .toUtc()
                  .toIso8601String(),
              deletedKey: true,
              idKey: 'some-other-id',
            },
          ),
          keyVersion: 1,
        ),
      ];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.title, 'sneaky');
        expect(row.updatedAt, wireUpdatedAt, reason: 'LWW key not spoofable');
        expect(row.deleted, isFalse, reason: 'tombstone not spoofable');
      });

      syncManager.dispose();
    });
  });

  group('Shadow-mode pull (plaintext authoritative)', () {
    Map<String, dynamic> shadowWireRow(
      FakeFieldCipher cipher, {
      required String id,
      required String userId,
      required String plaintextTitle,
      required String blobTitle,
      int amount = 7,
      int? keyVersion,
    }) {
      final version = keyVersion ?? cipher.activeKeyVersion;
      return {
        idKey: id,
        userIdKey: userId,
        updatedAtKey: DateTime.now().toUtc().toIso8601String(),
        deletedKey: false,
        circleIdKey: circleId,
        titleKey: plaintextTitle,
        amountKey: amount,
        assigneeKey: null,
        contentEncKey: cipher.blobFor(
          table: secretItemsTable,
          rowId: id,
          circleId: circleId,
          keyVersion: version,
          fields: {titleKey: blobTitle, amountKey: amount},
        ),
        keyVersionKey: version,
      };
    }

    test('parity holds: plaintext row lands, no alert', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.shadow
        ..keys[circleId] = {1};
      final alerts = <SyncEncryptionAlert>[];
      final syncManager = buildManager(
        cipher: cipher,
        alerts: alerts,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      backendRows = [
        shadowWireRow(
          cipher,
          id: id,
          userId: userId,
          plaintextTitle: 'same',
          blobTitle: 'same',
        ),
      ];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.title, 'same');
        expect(row.locked, isFalse);
      });
      expect(alerts, isEmpty);

      syncManager.dispose();
    });

    test(
      'parity mismatch: plaintext stays authoritative, a WARNING alert names '
      'the field but never the values',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.shadow
          ..keys[circleId] = {1};
        final alerts = <SyncEncryptionAlert>[];
        final syncManager = buildManager(
          cipher: cipher,
          alerts: alerts,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        final id = const Uuid().v4();
        backendRows = [
          shadowWireRow(
            cipher,
            id: id,
            userId: userId,
            plaintextTitle: 'authoritative plaintext',
            blobTitle: 'diverged blob value',
          ),
        ];

        await waitForFunctionToPass(() async {
          await syncManager.syncTables();
          final row = await localRow(id);
          expect(row.title, 'authoritative plaintext');
          expect(row.locked, isFalse);
        });
        // The same row version may decode more than once (reconcile/realtime
        // races) — every alert must be the parity mismatch, nothing else.
        expect(alerts, isNotEmpty);
        expect(alerts.map((a) => a.kind).toSet(), {
          SyncEncryptionAlertKind.shadowParityMismatch,
        });
        final alert = alerts.first;
        expect(alert.severity, SyncEncryptionAlertSeverity.warning);
        expect(alert.message, contains(titleKey));
        expect(alert.message, isNot(contains('authoritative plaintext')));
        expect(alert.message, isNot(contains('diverged blob value')));

        syncManager.dispose();
      },
    );

    test(
      'shadow + missing key: plaintext lands without alert (verification '
      "simply unavailable; key telemetry is not the pull path's job)",
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.shadow; // no keys
        final alerts = <SyncEncryptionAlert>[];
        final syncManager = buildManager(
          cipher: cipher,
          alerts: alerts,
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        final id = const Uuid().v4();
        backendRows = [
          shadowWireRow(
            cipher,
            id: id,
            userId: userId,
            plaintextTitle: 'readable',
            blobTitle: 'readable',
          ),
        ];

        await waitForFunctionToPass(() async {
          await syncManager.syncTables();
          final row = await localRow(id);
          expect(row.title, 'readable');
          expect(row.locked, isFalse, reason: 'readable rows are never locked');
        });
        expect(alerts, isEmpty);

        syncManager.dispose();
      },
    );

    test('shadow + auth failure: CRITICAL tampering alert, but the readable '
        'plaintext row still lands unlocked', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.shadow
        ..keys[circleId] = {1};
      final alerts = <SyncEncryptionAlert>[];
      final syncManager = buildManager(
        cipher: cipher,
        alerts: alerts,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      final row = shadowWireRow(
        cipher,
        id: id,
        userId: userId,
        plaintextTitle: 'still readable',
        blobTitle: 'whatever',
      );
      // Tamper: the blob is bound to a different row id.
      row[contentEncKey] = cipher.blobFor(
        table: secretItemsTable,
        rowId: 'other-row',
        circleId: circleId,
        keyVersion: 1,
        fields: const {titleKey: 'whatever', amountKey: 7},
      );
      backendRows = [row];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final local = await localRow(id);
        expect(local.title, 'still readable');
        expect(local.locked, isFalse);
      });
      expect(alerts, isNotEmpty);
      expect(alerts.map((a) => a.kind).toSet(), {
        SyncEncryptionAlertKind.possibleTampering,
      });
      expect(alerts.first.severity, SyncEncryptionAlertSeverity.critical);

      syncManager.dispose();
    });

    test('an enforced-shaped row (nulled plaintext) pulled while the mode says '
        'shadow is read from the blob — mixed-state read path', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.shadow
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      final id = const Uuid().v4();
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: DateTime.now().toUtc(),
          title: 'from blob',
          amount: 2,
        ),
      ];

      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(id);
        expect(row.title, 'from blob');
      });

      syncManager.dispose();
    });
  });

  group('Conflict resolution stays plaintext (LWW on updated_at)', () {
    test('an incoming encrypted row older than the local copy is discarded; a '
        'newer one wins — exactly as for plaintext rows', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();
      // Prevent the local row from being pushed while we drive pulls.
      syncManager.disableSync();

      final localTime = DateTime.now().toUtc();
      final item = await insertLocal(
        userId: userId,
        title: 'local edit',
        updatedAt: localTime,
      );

      syncManager.enableSync();

      // 1) Backend holds an OLDER encrypted version → local wins.
      backendRows = [
        enforcedWireRow(
          cipher,
          id: item.id,
          userId: userId,
          updatedAt: localTime.subtract(const Duration(minutes: 1)),
          title: 'stale backend',
        ),
      ];
      await syncManager.syncTables();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect((await localRow(item.id)).title, 'local edit');

      // 2) Backend holds a NEWER encrypted version → backend wins.
      backendRows = [
        enforcedWireRow(
          cipher,
          id: item.id,
          userId: userId,
          updatedAt: localTime.add(const Duration(minutes: 1)),
          title: 'fresher backend',
          amount: 1000,
        ),
      ];
      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        final row = await localRow(item.id);
        expect(row.title, 'fresher backend');
        expect(row.amount, 1000);
        expect(row.dirty, isFalse);
      });

      syncManager.dispose();
    });
  });

  group('Locked-row write semantics', () {
    test(
      'tombstoning a locked row pushes the ORIGINAL blob verbatim with '
      'nulled content fields — placeholders are never encrypted or pushed',
      () async {
        final cipher = FakeFieldCipher()
          ..circleModes[circleId] = SyncEncryptionMode.enforced; // no keys
        final syncManager = buildManager(
          cipher: cipher,
          alerts: <SyncEncryptionAlert>[],
          encryption: secretItemsEncryption(),
        );
        final userId = const Uuid().v4();
        syncManager.setUserId(userId);
        syncManager.enableSync();

        // A locked row arrives (key v7 unknown).
        final id = const Uuid().v4();
        final wire = enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: DateTime.now().toUtc(),
          keyVersion: 7,
        );
        backendRows = [wire];
        await waitForFunctionToPass(() async {
          await syncManager.syncTables();
          expect((await localRow(id)).locked, isTrue);
        });
        backendRows = [];

        // The user deletes it (a plaintext-column write — allowed on locked
        // rows). The app bumps updatedAt + dirty as for any local edit.
        await (testDb.update(
          testDb.secretItems,
        )..where((t) => t.id.equals(id))).write(
          SecretItemsCompanion(
            deleted: const drift.Value(true),
            updatedAt: drift.Value(
              DateTime.now().toUtc().add(const Duration(seconds: 1)),
            ),
            dirty: const drift.Value(true),
          ),
        );

        await waitForFunctionToPass(() async {
          expect(syncManager.nSyncedToBackend(SecretItem), 1);
        });

        final pushed = pushedRows().single;
        expect(pushed[idKey], id);
        expect(pushed[deletedKey], isTrue);
        expect(
          pushed[contentEncKey],
          wire[contentEncKey],
          reason: 'the original ciphertext must be forwarded verbatim',
        );
        expect(pushed[keyVersionKey], 7);
        expect(pushed[titleKey], isNull);
        expect(pushed[amountKey], isNull);
        expect(
          cipher.encryptCalls,
          isEmpty,
          reason: 'a locked row must never be a source for encryption',
        );

        syncManager.dispose();
      },
    );
  });

  group('Decrypt-and-restore (drop back to plaintext)', () {
    test("repushRowsForRestore re-pushes a circle's rows; with the mode back "
        'at off they go out as plaintext with nulled blob columns', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      // The circle's data was synced while enforced (local rows are clean).
      final id = const Uuid().v4();
      final originalUpdatedAt = DateTime.now().toUtc();
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: originalUpdatedAt,
          title: 'restore me',
          amount: 77,
        ),
      ];
      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        expect((await localRow(id)).title, 'restore me');
      });
      backendRows = [];
      expect(pushedRows(), isEmpty);

      // Admin drops the circle back to plaintext, then the seam rewrites
      // the backend rows from the locally decrypted copies.
      cipher.circleModes[circleId] = SyncEncryptionMode.off;
      final repushed = await syncManager.repushRowsForRestore(
        circleId: circleId,
      );
      expect(repushed, 1);

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });
      final pushed = pushedRows().single;
      expect(pushed[titleKey], 'restore me');
      expect(pushed[amountKey], 77);
      expect(pushed.containsKey(contentEncKey), isTrue);
      expect(pushed[contentEncKey], isNull);
      expect(pushed[keyVersionKey], isNull);
      expect(
        DateTime.parse(
          pushed[updatedAtKey] as String,
        ).isAfter(originalUpdatedAt),
        isTrue,
        reason:
            'restore must bump updated_at so the backend LWW trigger '
            'accepts the rewrite',
      );

      syncManager.dispose();
    });
  });

  group('Unlock & quarantine recovery (MC-430 review)', () {
    test('a tombstone written while an unlock decrypt is in flight survives '
        '— the unlock is version-guarded', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced;
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      // A row arrives undecryptable (no key yet) and locks.
      final id = const Uuid().v4();
      final blob = cipher.blobFor(
        table: secretItemsTable,
        rowId: id,
        circleId: circleId,
        keyVersion: 1,
        fields: const {titleKey: 'secret', amountKey: 5},
      );
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: DateTime.now().toUtc(),
          contentEnc: blob,
          keyVersion: 1,
        ),
      ];
      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        expect((await localRow(id)).locked, isTrue);
      });
      backendRows = [];

      // The key arrives; the unlock decrypt is artificially slow, and the
      // user swipe-deletes the locked row (allowed — tombstones forward the
      // preserved blob) while the decrypt is in flight.
      cipher.keys[circleId] = {1};
      cipher.blobDecryptDelays[blob] = const Duration(milliseconds: 150);
      final retry = syncManager.retryLockedRows();
      await Future<void>.delayed(const Duration(milliseconds: 30));
      final tombstoneAt = DateTime.now().toUtc();
      await (testDb.update(
        testDb.secretItems,
      )..where((t) => t.id.equals(id))).write(
        SecretItemsCompanion(
          deleted: const drift.Value(true),
          updatedAt: drift.Value(tombstoneAt),
          dirty: const drift.Value(true),
        ),
      );

      final unlocked = await retry;
      expect(unlocked, 0, reason: 'the row moved on — unlock must skip');
      final row = await localRow(id);
      expect(row.deleted, isTrue, reason: 'the tombstone must survive');
      expect(row.updatedAt.isAtSameMomentAs(tombstoneAt), isTrue);
      expect(
        row.locked,
        isTrue,
        reason: 'still locked — the next retry decrypts the fresh snapshot',
      );
      // The tombstone either already pushed (retryLockedRows re-enqueues
      // dirty rows, clearing the flag on success) or is still pending —
      // but it must never have been replaced by a resurrected push.
      if (!row.dirty) {
        expect(pushedRows(), isNotEmpty);
        expect(pushedRows().last[deletedKey], isTrue);
      }
      expect(
        pushedRows().where((r) => r[deletedKey] == false),
        isEmpty,
        reason: 'no resurrected (deleted=false) version may reach the wire',
      );

      syncManager.dispose();
    });

    test('retryLockedRows clears the outgoing quarantine of encrypted '
        'tables, so a push rejected under a stale mode retries once the '
        'world changes', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.shadow
        ..keys[circleId] = {1};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();

      // The backend permanently rejects the first two posts (the batch and
      // its per-row fallback) — modelling the enforced-mode plaintext guard
      // rejecting a stale shadow-shaped push.
      var failuresLeft = 2;
      when(mockHttpClient.send(any)).thenAnswer((invocation) async {
        final request = invocation.positionalArguments[0] as BaseRequest;
        if (request.method == 'POST') {
          final body = (request as Request).body;
          if (failuresLeft > 0) {
            failuresLeft--;
            final errorBody = jsonEncode({
              'code': '42501',
              'message': 'E2E_PLAINTEXT_REJECTED: stale mode',
              'details': 'Forbidden',
              'hint': null,
            });
            return StreamedResponse(
              Stream<List<int>>.value(utf8.encode(errorBody)),
              403,
              request: request,
              headers: {'content-type': 'application/json; charset=utf-8'},
            );
          }
          pushedBatches.add(
            (jsonDecode(body) as List).cast<Map<String, dynamic>>(),
          );
          return StreamedResponse(
            Stream<List<int>>.value(utf8.encode(body)),
            200,
            request: request,
            headers: {'content-type': 'application/json; charset=utf-8'},
          );
        }

        return StreamedResponse(
          Stream<List<int>>.value(utf8.encode(jsonEncode(backendRows))),
          200,
          request: request,
          headers: {'content-type': 'application/json; charset=utf-8'},
        );
      });

      syncManager.setUserId(userId);
      syncManager.enableSync();
      await insertLocal(userId: userId, title: 'stranded edit');

      // The push fails permanently and the row is quarantined: further
      // passes attempt nothing (no successful post recorded).
      await waitForFunctionToPass(() async {
        expect(failuresLeft, 0);
      });
      await syncManager.syncTables();
      expect(pushedRows(), isEmpty);

      // The world changes: the circle is now known to be enforced. The
      // key/mode-change retry hook must clear the quarantine so the row
      // re-encodes (enforced-shaped) and finally lands.
      cipher.circleModes[circleId] = SyncEncryptionMode.enforced;
      await syncManager.retryLockedRows();
      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });
      final pushed = pushedRows().single;
      expect(pushed[titleKey], isNull);
      expect(pushed[contentEncKey], isNotNull);

      syncManager.dispose();
    });
  });

  group('Re-encrypt sweep (repushRowsForReencrypt)', () {
    test("re-pushes a circle's rows under the active key WITHOUT bumping "
        'updated_at; locked rows are skipped', () async {
      final cipher = FakeFieldCipher()
        ..circleModes[circleId] = SyncEncryptionMode.enforced
        ..keys[circleId] = {1, 2};
      final syncManager = buildManager(
        cipher: cipher,
        encryption: secretItemsEncryption(),
      );
      final userId = const Uuid().v4();
      syncManager.setUserId(userId);
      syncManager.enableSync();

      // The circle synced while v1 was active (local row is clean), then the
      // key rotated to v2 — the sweep must rewrite the backend blob.
      final id = const Uuid().v4();
      final originalUpdatedAt = DateTime.now().toUtc();
      backendRows = [
        enforcedWireRow(
          cipher,
          id: id,
          userId: userId,
          updatedAt: originalUpdatedAt,
          title: 'sweep me',
          amount: 12,
          keyVersion: 1,
        ),
      ];
      await waitForFunctionToPass(() async {
        await syncManager.syncTables();
        expect((await localRow(id)).title, 'sweep me');
      });
      backendRows = [];
      expect(pushedRows(), isEmpty);

      // A locked row of the same circle must never be marked for re-push:
      // its content fields are placeholders, not a source for encryption
      // (the seam would forward its preserved blob verbatim — pointless
      // churn the sweep skips outright).
      await testDb
          .into(testDb.secretItems)
          .insert(
            SecretItemsCompanion(
              id: drift.Value(const Uuid().v4()),
              userId: drift.Value(userId),
              updatedAt: drift.Value(DateTime.now().toUtc()),
              circleId: drift.Value(circleId),
              title: const drift.Value('🔒'),
              amount: const drift.Value(0),
              dirty: const drift.Value(false),
              locked: const drift.Value(true),
              lockedContentEnc: const drift.Value('preserved-blob'),
              lockedKeyVersion: const drift.Value(9),
            ),
          );

      cipher.activeKeyVersion = 2;
      final marked = await syncManager.repushRowsForReencrypt(
        circleId: circleId,
      );
      expect(marked, 1, reason: 'the locked row must be skipped');

      await waitForFunctionToPass(() async {
        expect(syncManager.nSyncedToBackend(SecretItem), 1);
      });
      final pushed = pushedRows().single;
      expect(pushed[idKey], id);
      expect(pushed[titleKey], isNull);
      expect(pushed[amountKey], isNull);
      expect(pushed[keyVersionKey], 2);
      final blob =
          jsonDecode(utf8.decode(base64Decode(pushed[contentEncKey] as String)))
              as Map<String, dynamic>;
      expect(blob['fields'], {titleKey: 'sweep me', amountKey: 12});
      expect(
        DateTime.parse(
          pushed[updatedAtKey] as String,
        ).isAtSameMomentAs(originalUpdatedAt),
        isTrue,
        reason:
            'a re-encrypt is not an edit: updated_at must NOT be bumped, so '
            'other devices do not re-pull unchanged content and the sweep '
            'never wins an LWW race against a genuine concurrent edit',
      );

      syncManager.dispose();
    });
  });

  group('Registration validation', () {
    test('encryption requires a field cipher on the manager', () {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
      );
      expect(
        () => syncManager.registerSyncable<SecretItem>(
          backendTable: secretItemsTable,
          fromJson: SecretItem.fromJson,
          companionConstructor: SecretItemsCompanion.new,
          encryption: secretItemsEncryption(),
        ),
        throwsException,
      );
    });

    test('protected sync columns can never be registered as encrypted', () {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        fieldCipher: FakeFieldCipher(),
      );
      for (final protected in [
        idKey,
        userIdKey,
        updatedAtKey,
        deletedKey,
        circleIdKey,
        contentEncKey,
        keyVersionKey,
      ]) {
        expect(
          () => syncManager.registerSyncable<SecretItem>(
            backendTable: secretItemsTable,
            fromJson: SecretItem.fromJson,
            companionConstructor: SecretItemsCompanion.new,
            encryption: SyncEncryption(
              encryptedFields: {titleKey, protected},
              lockedFieldPlaceholders: const {titleKey: '🔒'},
            ),
          ),
          throwsException,
          reason: '$protected must be rejected',
        );
      }
    });

    test('an empty encrypted-field set is rejected', () {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        fieldCipher: FakeFieldCipher(),
      );
      expect(
        () => syncManager.registerSyncable<SecretItem>(
          backendTable: secretItemsTable,
          fromJson: SecretItem.fromJson,
          companionConstructor: SecretItemsCompanion.new,
          encryption: SyncEncryption(
            encryptedFields: const {},
            lockedFieldPlaceholders: const {},
          ),
        ),
        throwsException,
      );
    });

    test('placeholders for unregistered fields are rejected', () {
      final syncManager = SyncManager<TestDatabase>(
        localDatabase: testDb,
        supabaseClient: mockSupabaseClient,
        syncInterval: const Duration(milliseconds: 1),
        fieldCipher: FakeFieldCipher(),
      );
      expect(
        () => syncManager.registerSyncable<SecretItem>(
          backendTable: secretItemsTable,
          fromJson: SecretItem.fromJson,
          companionConstructor: SecretItemsCompanion.new,
          encryption: SyncEncryption(
            encryptedFields: const {titleKey},
            lockedFieldPlaceholders: const {titleKey: '🔒', amountKey: 0},
          ),
        ),
        throwsException,
      );
    });

    test(
      'a table without the locked-row columns cannot register encryption',
      () {
        final syncManager = SyncManager<TestDatabase>(
          localDatabase: testDb,
          supabaseClient: mockSupabaseClient,
          syncInterval: const Duration(milliseconds: 1),
          fieldCipher: FakeFieldCipher(),
        );
        expect(
          () => syncManager.registerSyncable<Item>(
            backendTable: itemsTable,
            fromJson: Item.fromJson,
            companionConstructor: ItemsCompanion.new,
            encryption: SyncEncryption(
              encryptedFields: const {nameKey},
              lockedFieldPlaceholders: const {nameKey: '🔒'},
            ),
          ),
          throwsException,
        );
      },
    );
  });
}
