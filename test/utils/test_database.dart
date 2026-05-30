import 'package:drift/drift.dart' hide JsonKey;
import 'package:equatable/equatable.dart';
import 'package:json_annotation/json_annotation.dart';
import 'package:syncable/syncable.dart';
import 'package:uuid/uuid.dart';

part 'test_database.g.dart';

@DriftDatabase(tables: [Items])
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
