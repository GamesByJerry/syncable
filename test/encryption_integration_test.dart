import 'package:drift/drift.dart' hide isNotNull, isNull;
import 'package:drift/native.dart';
import 'package:supabase/supabase.dart';
import 'package:syncable/src/supabase_names.dart';
import 'package:syncable/syncable.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import 'utils/fake_field_cipher.dart';
import 'utils/test_database.dart';
import 'utils/test_supabase_names.dart';
import 'utils/wait_for_function_to_pass.dart';

/// MC-427 spike verification against a real Supabase stack: the encrypted
/// round-trip (typed Postgres columns accept the blob payload, plaintext
/// columns are genuinely nulled server-side), realtime decrypt-on-receipt,
/// and locked-row recovery — across two devices.
void main() {
  // Two "devices": each with its own local database, sync manager, and
  // cipher. They share the same Supabase backend and user. The multi-database
  // warning is a false positive here — the databases use separate in-memory
  // executors on purpose.
  driftRuntimeOptions.dontWarnAboutMultipleDatabases = true;

  late TestDatabase deviceADb;
  late TestDatabase deviceBDb;

  final supabaseClient = SupabaseClient(
    'http://127.0.0.1:54321',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'
        'eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6ImFub24iLCJleHAiOjE5ODM4MTI5OTZ9.'
        'CRXP1A7WOeoJeXxjNni43kdQwgnWNReilDMblYTn_I0',
  );

  late FakeFieldCipher cipherA;
  late FakeFieldCipher cipherB;
  late SyncManager<TestDatabase> managerA;
  late SyncManager<TestDatabase> managerB;
  late List<SyncEncryptionAlert> alertsB;

  final circleId = const Uuid().v4();

  SyncManager<TestDatabase> buildManager(
    TestDatabase db,
    FakeFieldCipher cipher,
    void Function(SyncEncryptionAlert)? onAlert,
  ) {
    final manager = SyncManager<TestDatabase>(
      localDatabase: db,
      supabaseClient: supabaseClient,
      fieldCipher: cipher,
      onEncryptionAlert: onAlert,
    );
    manager.registerSyncable<SecretItem>(
      backendTable: secretItemsTable,
      fromJson: SecretItem.fromJson,
      companionConstructor: SecretItemsCompanion.new,
      encryption: SyncEncryption(
        encryptedFields: const {titleKey, amountKey},
        lockedFieldPlaceholders: const {titleKey: '🔒', amountKey: 0},
      ),
    );
    return manager;
  }

  setUp(() async {
    await supabaseClient.auth.signOut();

    deviceADb = TestDatabase(
      DatabaseConnection(
        NativeDatabase.memory(),
        closeStreamsSynchronously: true,
      ),
    );
    deviceBDb = TestDatabase(
      DatabaseConnection(
        NativeDatabase.memory(),
        closeStreamsSynchronously: true,
      ),
    );

    cipherA = FakeFieldCipher()
      ..circleModes[circleId] = SyncEncryptionMode.enforced
      ..keys[circleId] = {1};
    cipherB = FakeFieldCipher()
      ..circleModes[circleId] = SyncEncryptionMode.enforced
      ..keys[circleId] = {1};

    alertsB = [];
    managerA = buildManager(deviceADb, cipherA, null);
    managerB = buildManager(deviceBDb, cipherB, alertsB.add);
  });

  tearDown(() async {
    managerA.dispose();
    managerB.dispose();
    await supabaseClient.auth.signOut();
    await deviceADb.close();
    await deviceBDb.close();
  });

  test('Encrypted round-trip across devices: ciphertext-only on the server, '
      'plaintext on both ends, realtime decrypt on receipt', () async {
    await supabaseClient.auth.signInAnonymously();
    final userId = supabaseClient.auth.currentUser!.id;

    managerA.setUserId(userId);
    managerA.enableSync();
    managerB.setUserId(userId);
    managerB.enableSync();

    // Device A creates a row in the enforced circle.
    final item = await deviceADb
        .into(deviceADb.secretItems)
        .insertReturning(
          SecretItemsCompanion(
            userId: Value(userId),
            updatedAt: Value(DateTime.now().toUtc()),
            circleId: Value(circleId),
            title: const Value('plaintext never leaves'),
            amount: const Value(42),
          ),
        );

    // The server holds ciphertext only: typed columns accepted the payload,
    // the content columns are genuinely null, the envelope is intact.
    await waitForFunctionToPass(() async {
      final serverRows = await supabaseClient
          .from(secretItemsTable)
          .select()
          .eq(idKey, item.id);
      expect(serverRows, hasLength(1));
      final serverRow = serverRows.single;
      expect(serverRow[titleKey], isNull);
      expect(serverRow[amountKey], isNull);
      expect(serverRow[contentEncKey], isNotNull);
      expect(serverRow[keyVersionKey], 1);
      expect(serverRow[circleIdKey], circleId);
      expect(serverRow[userIdKey], userId);
    });

    // Device B ends up with the decrypted plaintext (pull or realtime).
    await waitForFunctionToPass(() async {
      final row = await deviceBDb.getItem(deviceBDb.secretItems, item.id);
      expect(row.title, 'plaintext never leaves');
      expect(row.amount, 42);
      expect(row.locked, isFalse);
      expect(row.dirty, isFalse);
    });

    // Device A edits the row; device B must see the new content decrypt on
    // receipt — through the live realtime channel.
    expect(managerB.isSubscribedToBackend, isTrue);
    await (deviceADb.update(
      deviceADb.secretItems,
    )..where((t) => t.id.equals(item.id))).write(
      SecretItemsCompanion(
        title: const Value('updated secret'),
        updatedAt: Value(DateTime.now().toUtc()),
        dirty: const Value(true),
      ),
    );

    await waitForFunctionToPass(() async {
      final row = await deviceBDb.getItem(deviceBDb.secretItems, item.id);
      expect(row.title, 'updated secret');
    }, timeout: const Duration(seconds: 15));

    expect(alertsB, isEmpty);
  });

  test('A device without the circle key stores rows locked and recovers them '
      'locally once the key arrives', () async {
    await supabaseClient.auth.signInAnonymously();
    final userId = supabaseClient.auth.currentUser!.id;

    // Device B has no key for the circle (it never received a wrap).
    cipherB.keys.clear();

    managerA.setUserId(userId);
    managerA.enableSync();
    managerB.setUserId(userId);
    managerB.enableSync();

    final item = await deviceADb
        .into(deviceADb.secretItems)
        .insertReturning(
          SecretItemsCompanion(
            userId: Value(userId),
            updatedAt: Value(DateTime.now().toUtc()),
            circleId: Value(circleId),
            title: const Value('locked for B'),
            amount: const Value(7),
          ),
        );

    // B receives the row but cannot read it: locked placeholder, preserved
    // ciphertext, info alert.
    await waitForFunctionToPass(() async {
      final row = await deviceBDb.getItem(deviceBDb.secretItems, item.id);
      expect(row.locked, isTrue);
      expect(row.title, '🔒');
      expect(row.lockedContentEnc, isNotNull);
      expect(row.lockedKeyVersion, 1);
    });
    expect(
      alertsB.map((a) => a.kind),
      contains(SyncEncryptionAlertKind.keyUnavailable),
    );
    expect(alertsB.map((a) => a.severity).toSet(), {
      SyncEncryptionAlertSeverity.info,
    });

    // The key arrives (MC-428's job) — recovery is local-only.
    cipherB.keys[circleId] = {1};
    final unlocked = await managerB.retryLockedRows();
    expect(unlocked, 1);

    final row = await deviceBDb.getItem(deviceBDb.secretItems, item.id);
    expect(row.locked, isFalse);
    expect(row.title, 'locked for B');
    expect(row.amount, 7);
    expect(row.lockedContentEnc, isNull);
    expect(row.dirty, isFalse);
  });
}
