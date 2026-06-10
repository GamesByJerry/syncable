import 'package:drift/drift.dart' hide JsonKey;
import 'package:equatable/equatable.dart';
import 'package:json_annotation/json_annotation.dart';
import 'package:syncable/syncable.dart';
import 'package:uuid/uuid.dart';

part 'test_database.g.dart';

@DriftDatabase(tables: [Items, SecretItems])
class TestDatabase extends _$TestDatabase with SyncableDatabase {
  TestDatabase(super.executor);

  @override
  int get schemaVersion => 1;
}

@UseRowClass(Item)
class Items extends Table implements SyncableTable {
  @override
  TextColumn get id => text().clientDefault(() => const Uuid().v4())();
  @override
  TextColumn get userId => text().withLength(min: 36, max: 36).nullable()();
  @override
  DateTimeColumn get updatedAt => dateTime()();
  @override
  BoolColumn get deleted => boolean().withDefault(const Constant(false))();

  @override
  BoolColumn get dirty => boolean().withDefault(const Constant(true))();

  TextColumn get name => text().withLength(min: 1, max: 50)();

  @override
  Set<Column> get primaryKey => {id};
}

@JsonSerializable()
class Item extends Equatable implements Syncable {
  const Item({
    required this.id,
    required this.userId,
    required this.updatedAt,
    required this.deleted,
    required this.name,
    this.dirty = true,
  });

  factory Item.fromJson(Map<String, dynamic> json) => _$ItemFromJson(json);

  @override
  final String id;
  @override
  final String? userId;
  @override
  final DateTime updatedAt;
  @override
  final bool deleted;

  // Local-only push flag — never crosses the wire (backend has no such column).
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  final bool dirty;

  final String name;

  @override
  List<Object?> get props => [id, userId, updatedAt];

  @override
  bool get stringify => true;

  @override
  Map<String, dynamic> toJson() => _$ItemToJson(this);

  @override
  UpdateCompanion<Item> toCompanion() {
    return ItemsCompanion.insert(
          id: Value(id),
          updatedAt: updatedAt,
          userId: Value(userId),
          deleted: Value(deleted),
          dirty: Value(dirty),
          name: name,
        )
        as UpdateCompanion<Item>;
  }
}

/// A circle-scoped table registered for field encryption in the seam tests
/// (MC-427). `title` and `amount` are the encrypted content fields (`amount`
/// being an int proves typed columns survive the blob round-trip); `assignee`
/// stands in for a plaintext FK-style content column that must never enter
/// the blob.
@UseRowClass(SecretItem)
class SecretItems extends Table implements EncryptedSyncableTable {
  @override
  TextColumn get id => text().clientDefault(() => const Uuid().v4())();
  @override
  TextColumn get userId => text().withLength(min: 36, max: 36).nullable()();
  @override
  DateTimeColumn get updatedAt => dateTime()();
  @override
  BoolColumn get deleted => boolean().withDefault(const Constant(false))();

  @override
  BoolColumn get dirty => boolean().withDefault(const Constant(true))();

  @override
  BoolColumn get locked => boolean().withDefault(const Constant(false))();
  @override
  TextColumn get lockedContentEnc => text().nullable()();
  @override
  IntColumn get lockedKeyVersion => integer().nullable()();

  TextColumn get circleId => text().nullable()();
  TextColumn get title => text()();
  IntColumn get amount => integer()();
  TextColumn get assignee => text().nullable()();

  @override
  Set<Column> get primaryKey => {id};
}

@JsonSerializable()
class SecretItem extends Equatable implements EncryptedSyncable {
  const SecretItem({
    required this.id,
    required this.userId,
    required this.updatedAt,
    required this.deleted,
    required this.circleId,
    required this.title,
    required this.amount,
    this.assignee,
    this.dirty = true,
    this.locked = false,
    this.lockedContentEnc,
    this.lockedKeyVersion,
  });

  factory SecretItem.fromJson(Map<String, dynamic> json) =>
      _$SecretItemFromJson(json);

  @override
  final String id;
  @override
  final String? userId;
  @override
  final DateTime updatedAt;
  @override
  final bool deleted;

  // Local-only push flag — never crosses the wire (backend has no such column).
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  final bool dirty;

  // Local-only locked-row state — the seam owns the wire representation of
  // the ciphertext, and the backend must not be able to inject locked state.
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  final bool locked;
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  final String? lockedContentEnc;
  @override
  @JsonKey(includeFromJson: false, includeToJson: false)
  final int? lockedKeyVersion;

  final String? circleId;
  final String title;
  final int amount;
  final String? assignee;

  @override
  List<Object?> get props => [id, userId, updatedAt];

  @override
  bool get stringify => true;

  @override
  Map<String, dynamic> toJson() => _$SecretItemToJson(this);

  @override
  UpdateCompanion<SecretItem> toCompanion() {
    return SecretItemsCompanion.insert(
          id: Value(id),
          updatedAt: updatedAt,
          userId: Value(userId),
          deleted: Value(deleted),
          dirty: Value(dirty),
          locked: Value(locked),
          lockedContentEnc: Value(lockedContentEnc),
          lockedKeyVersion: Value(lockedKeyVersion),
          circleId: Value(circleId),
          title: title,
          amount: amount,
          assignee: Value(assignee),
        )
        as UpdateCompanion<SecretItem>;
  }
}
