// GENERATED CODE - DO NOT MODIFY BY HAND

// coverage:ignore-file

part of 'test_database.dart';

// ignore_for_file: type=lint
class $ItemsTable extends Items with TableInfo<$ItemsTable, Item> {
  @override
  final GeneratedDatabase attachedDatabase;
  final String? _alias;
  $ItemsTable(this.attachedDatabase, [this._alias]);
  static const VerificationMeta _idMeta = const VerificationMeta('id');
  @override
  late final GeneratedColumn<String> id = GeneratedColumn<String>(
    'id',
    aliasedName,
    false,
    type: DriftSqlType.string,
    requiredDuringInsert: false,
    clientDefault: () => const Uuid().v4(),
  );
  static const VerificationMeta _userIdMeta = const VerificationMeta('userId');
  @override
  late final GeneratedColumn<String> userId = GeneratedColumn<String>(
    'user_id',
    aliasedName,
    true,
    additionalChecks: GeneratedColumn.checkTextLength(
      minTextLength: 36,
      maxTextLength: 36,
    ),
    type: DriftSqlType.string,
    requiredDuringInsert: false,
  );
  static const VerificationMeta _updatedAtMeta = const VerificationMeta(
    'updatedAt',
  );
  @override
  late final GeneratedColumn<DateTime> updatedAt = GeneratedColumn<DateTime>(
    'updated_at',
    aliasedName,
    false,
    type: DriftSqlType.dateTime,
    requiredDuringInsert: true,
  );
  static const VerificationMeta _deletedMeta = const VerificationMeta(
    'deleted',
  );
  @override
  late final GeneratedColumn<bool> deleted = GeneratedColumn<bool>(
    'deleted',
    aliasedName,
    false,
    type: DriftSqlType.bool,
    requiredDuringInsert: false,
    defaultConstraints: GeneratedColumn.constraintIsAlways(
      'CHECK ("deleted" IN (0, 1))',
    ),
    defaultValue: const Constant(false),
  );
  static const VerificationMeta _dirtyMeta = const VerificationMeta('dirty');
  @override
  late final GeneratedColumn<bool> dirty = GeneratedColumn<bool>(
    'dirty',
    aliasedName,
    false,
    type: DriftSqlType.bool,
    requiredDuringInsert: false,
    defaultConstraints: GeneratedColumn.constraintIsAlways(
      'CHECK ("dirty" IN (0, 1))',
    ),
    defaultValue: const Constant(true),
  );
  static const VerificationMeta _nameMeta = const VerificationMeta('name');
  @override
  late final GeneratedColumn<String> name = GeneratedColumn<String>(
    'name',
    aliasedName,
    false,
    additionalChecks: GeneratedColumn.checkTextLength(
      minTextLength: 1,
      maxTextLength: 50,
    ),
    type: DriftSqlType.string,
    requiredDuringInsert: true,
  );
  @override
  List<GeneratedColumn> get $columns => [
    id,
    userId,
    updatedAt,
    deleted,
    dirty,
    name,
  ];
  @override
  String get aliasedName => _alias ?? actualTableName;
  @override
  String get actualTableName => $name;
  static const String $name = 'items';
  @override
  VerificationContext validateIntegrity(
    Insertable<Item> instance, {
    bool isInserting = false,
  }) {
    final context = VerificationContext();
    final data = instance.toColumns(true);
    if (data.containsKey('id')) {
      context.handle(_idMeta, id.isAcceptableOrUnknown(data['id']!, _idMeta));
    }
    if (data.containsKey('user_id')) {
      context.handle(
        _userIdMeta,
        userId.isAcceptableOrUnknown(data['user_id']!, _userIdMeta),
      );
    }
    if (data.containsKey('updated_at')) {
      context.handle(
        _updatedAtMeta,
        updatedAt.isAcceptableOrUnknown(data['updated_at']!, _updatedAtMeta),
      );
    } else if (isInserting) {
      context.missing(_updatedAtMeta);
    }
    if (data.containsKey('deleted')) {
      context.handle(
        _deletedMeta,
        deleted.isAcceptableOrUnknown(data['deleted']!, _deletedMeta),
      );
    }
    if (data.containsKey('dirty')) {
      context.handle(
        _dirtyMeta,
        dirty.isAcceptableOrUnknown(data['dirty']!, _dirtyMeta),
      );
    }
    if (data.containsKey('name')) {
      context.handle(
        _nameMeta,
        name.isAcceptableOrUnknown(data['name']!, _nameMeta),
      );
    } else if (isInserting) {
      context.missing(_nameMeta);
    }
    return context;
  }

  @override
  Set<GeneratedColumn> get $primaryKey => {id};
  @override
  Item map(Map<String, dynamic> data, {String? tablePrefix}) {
    final effectivePrefix = tablePrefix != null ? '$tablePrefix.' : '';
    return Item(
      id: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}id'],
      )!,
      userId: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}user_id'],
      ),
      updatedAt: attachedDatabase.typeMapping.read(
        DriftSqlType.dateTime,
        data['${effectivePrefix}updated_at'],
      )!,
      deleted: attachedDatabase.typeMapping.read(
        DriftSqlType.bool,
        data['${effectivePrefix}deleted'],
      )!,
      name: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}name'],
      )!,
      dirty: attachedDatabase.typeMapping.read(
        DriftSqlType.bool,
        data['${effectivePrefix}dirty'],
      )!,
    );
  }

  @override
  $ItemsTable createAlias(String alias) {
    return $ItemsTable(attachedDatabase, alias);
  }
}

class ItemsCompanion extends UpdateCompanion<Item> {
  final Value<String> id;
  final Value<String?> userId;
  final Value<DateTime> updatedAt;
  final Value<bool> deleted;
  final Value<bool> dirty;
  final Value<String> name;
  final Value<int> rowid;
  const ItemsCompanion({
    this.id = const Value.absent(),
    this.userId = const Value.absent(),
    this.updatedAt = const Value.absent(),
    this.deleted = const Value.absent(),
    this.dirty = const Value.absent(),
    this.name = const Value.absent(),
    this.rowid = const Value.absent(),
  });
  ItemsCompanion.insert({
    this.id = const Value.absent(),
    this.userId = const Value.absent(),
    required DateTime updatedAt,
    this.deleted = const Value.absent(),
    this.dirty = const Value.absent(),
    required String name,
    this.rowid = const Value.absent(),
  }) : updatedAt = Value(updatedAt),
       name = Value(name);
  static Insertable<Item> custom({
    Expression<String>? id,
    Expression<String>? userId,
    Expression<DateTime>? updatedAt,
    Expression<bool>? deleted,
    Expression<bool>? dirty,
    Expression<String>? name,
    Expression<int>? rowid,
  }) {
    return RawValuesInsertable({
      if (id != null) 'id': id,
      if (userId != null) 'user_id': userId,
      if (updatedAt != null) 'updated_at': updatedAt,
      if (deleted != null) 'deleted': deleted,
      if (dirty != null) 'dirty': dirty,
      if (name != null) 'name': name,
      if (rowid != null) 'rowid': rowid,
    });
  }

  ItemsCompanion copyWith({
    Value<String>? id,
    Value<String?>? userId,
    Value<DateTime>? updatedAt,
    Value<bool>? deleted,
    Value<bool>? dirty,
    Value<String>? name,
    Value<int>? rowid,
  }) {
    return ItemsCompanion(
      id: id ?? this.id,
      userId: userId ?? this.userId,
      updatedAt: updatedAt ?? this.updatedAt,
      deleted: deleted ?? this.deleted,
      dirty: dirty ?? this.dirty,
      name: name ?? this.name,
      rowid: rowid ?? this.rowid,
    );
  }

  @override
  Map<String, Expression> toColumns(bool nullToAbsent) {
    final map = <String, Expression>{};
    if (id.present) {
      map['id'] = Variable<String>(id.value);
    }
    if (userId.present) {
      map['user_id'] = Variable<String>(userId.value);
    }
    if (updatedAt.present) {
      map['updated_at'] = Variable<DateTime>(updatedAt.value);
    }
    if (deleted.present) {
      map['deleted'] = Variable<bool>(deleted.value);
    }
    if (dirty.present) {
      map['dirty'] = Variable<bool>(dirty.value);
    }
    if (name.present) {
      map['name'] = Variable<String>(name.value);
    }
    if (rowid.present) {
      map['rowid'] = Variable<int>(rowid.value);
    }
    return map;
  }

  @override
  String toString() {
    return (StringBuffer('ItemsCompanion(')
          ..write('id: $id, ')
          ..write('userId: $userId, ')
          ..write('updatedAt: $updatedAt, ')
          ..write('deleted: $deleted, ')
          ..write('dirty: $dirty, ')
          ..write('name: $name, ')
          ..write('rowid: $rowid')
          ..write(')'))
        .toString();
  }
}

class $SecretItemsTable extends SecretItems
    with TableInfo<$SecretItemsTable, SecretItem> {
  @override
  final GeneratedDatabase attachedDatabase;
  final String? _alias;
  $SecretItemsTable(this.attachedDatabase, [this._alias]);
  static const VerificationMeta _lockedMeta = const VerificationMeta('locked');
  @override
  late final GeneratedColumn<bool> locked = GeneratedColumn<bool>(
    'locked',
    aliasedName,
    false,
    type: DriftSqlType.bool,
    requiredDuringInsert: false,
    defaultConstraints: GeneratedColumn.constraintIsAlways(
      'CHECK ("locked" IN (0, 1))',
    ),
    defaultValue: const Constant(false),
  );
  static const VerificationMeta _lockedContentEncMeta = const VerificationMeta(
    'lockedContentEnc',
  );
  @override
  late final GeneratedColumn<String> lockedContentEnc = GeneratedColumn<String>(
    'locked_content_enc',
    aliasedName,
    true,
    type: DriftSqlType.string,
    requiredDuringInsert: false,
  );
  static const VerificationMeta _lockedKeyVersionMeta = const VerificationMeta(
    'lockedKeyVersion',
  );
  @override
  late final GeneratedColumn<int> lockedKeyVersion = GeneratedColumn<int>(
    'locked_key_version',
    aliasedName,
    true,
    type: DriftSqlType.int,
    requiredDuringInsert: false,
  );
  static const VerificationMeta _idMeta = const VerificationMeta('id');
  @override
  late final GeneratedColumn<String> id = GeneratedColumn<String>(
    'id',
    aliasedName,
    false,
    type: DriftSqlType.string,
    requiredDuringInsert: false,
    clientDefault: () => const Uuid().v4(),
  );
  static const VerificationMeta _userIdMeta = const VerificationMeta('userId');
  @override
  late final GeneratedColumn<String> userId = GeneratedColumn<String>(
    'user_id',
    aliasedName,
    true,
    additionalChecks: GeneratedColumn.checkTextLength(
      minTextLength: 36,
      maxTextLength: 36,
    ),
    type: DriftSqlType.string,
    requiredDuringInsert: false,
  );
  static const VerificationMeta _updatedAtMeta = const VerificationMeta(
    'updatedAt',
  );
  @override
  late final GeneratedColumn<DateTime> updatedAt = GeneratedColumn<DateTime>(
    'updated_at',
    aliasedName,
    false,
    type: DriftSqlType.dateTime,
    requiredDuringInsert: true,
  );
  static const VerificationMeta _deletedMeta = const VerificationMeta(
    'deleted',
  );
  @override
  late final GeneratedColumn<bool> deleted = GeneratedColumn<bool>(
    'deleted',
    aliasedName,
    false,
    type: DriftSqlType.bool,
    requiredDuringInsert: false,
    defaultConstraints: GeneratedColumn.constraintIsAlways(
      'CHECK ("deleted" IN (0, 1))',
    ),
    defaultValue: const Constant(false),
  );
  static const VerificationMeta _dirtyMeta = const VerificationMeta('dirty');
  @override
  late final GeneratedColumn<bool> dirty = GeneratedColumn<bool>(
    'dirty',
    aliasedName,
    false,
    type: DriftSqlType.bool,
    requiredDuringInsert: false,
    defaultConstraints: GeneratedColumn.constraintIsAlways(
      'CHECK ("dirty" IN (0, 1))',
    ),
    defaultValue: const Constant(true),
  );
  static const VerificationMeta _circleIdMeta = const VerificationMeta(
    'circleId',
  );
  @override
  late final GeneratedColumn<String> circleId = GeneratedColumn<String>(
    'circle_id',
    aliasedName,
    true,
    type: DriftSqlType.string,
    requiredDuringInsert: false,
  );
  static const VerificationMeta _titleMeta = const VerificationMeta('title');
  @override
  late final GeneratedColumn<String> title = GeneratedColumn<String>(
    'title',
    aliasedName,
    false,
    type: DriftSqlType.string,
    requiredDuringInsert: true,
  );
  static const VerificationMeta _amountMeta = const VerificationMeta('amount');
  @override
  late final GeneratedColumn<int> amount = GeneratedColumn<int>(
    'amount',
    aliasedName,
    false,
    type: DriftSqlType.int,
    requiredDuringInsert: true,
  );
  static const VerificationMeta _assigneeMeta = const VerificationMeta(
    'assignee',
  );
  @override
  late final GeneratedColumn<String> assignee = GeneratedColumn<String>(
    'assignee',
    aliasedName,
    true,
    type: DriftSqlType.string,
    requiredDuringInsert: false,
  );
  @override
  List<GeneratedColumn> get $columns => [
    locked,
    lockedContentEnc,
    lockedKeyVersion,
    id,
    userId,
    updatedAt,
    deleted,
    dirty,
    circleId,
    title,
    amount,
    assignee,
  ];
  @override
  String get aliasedName => _alias ?? actualTableName;
  @override
  String get actualTableName => $name;
  static const String $name = 'secret_items';
  @override
  VerificationContext validateIntegrity(
    Insertable<SecretItem> instance, {
    bool isInserting = false,
  }) {
    final context = VerificationContext();
    final data = instance.toColumns(true);
    if (data.containsKey('locked')) {
      context.handle(
        _lockedMeta,
        locked.isAcceptableOrUnknown(data['locked']!, _lockedMeta),
      );
    }
    if (data.containsKey('locked_content_enc')) {
      context.handle(
        _lockedContentEncMeta,
        lockedContentEnc.isAcceptableOrUnknown(
          data['locked_content_enc']!,
          _lockedContentEncMeta,
        ),
      );
    }
    if (data.containsKey('locked_key_version')) {
      context.handle(
        _lockedKeyVersionMeta,
        lockedKeyVersion.isAcceptableOrUnknown(
          data['locked_key_version']!,
          _lockedKeyVersionMeta,
        ),
      );
    }
    if (data.containsKey('id')) {
      context.handle(_idMeta, id.isAcceptableOrUnknown(data['id']!, _idMeta));
    }
    if (data.containsKey('user_id')) {
      context.handle(
        _userIdMeta,
        userId.isAcceptableOrUnknown(data['user_id']!, _userIdMeta),
      );
    }
    if (data.containsKey('updated_at')) {
      context.handle(
        _updatedAtMeta,
        updatedAt.isAcceptableOrUnknown(data['updated_at']!, _updatedAtMeta),
      );
    } else if (isInserting) {
      context.missing(_updatedAtMeta);
    }
    if (data.containsKey('deleted')) {
      context.handle(
        _deletedMeta,
        deleted.isAcceptableOrUnknown(data['deleted']!, _deletedMeta),
      );
    }
    if (data.containsKey('dirty')) {
      context.handle(
        _dirtyMeta,
        dirty.isAcceptableOrUnknown(data['dirty']!, _dirtyMeta),
      );
    }
    if (data.containsKey('circle_id')) {
      context.handle(
        _circleIdMeta,
        circleId.isAcceptableOrUnknown(data['circle_id']!, _circleIdMeta),
      );
    }
    if (data.containsKey('title')) {
      context.handle(
        _titleMeta,
        title.isAcceptableOrUnknown(data['title']!, _titleMeta),
      );
    } else if (isInserting) {
      context.missing(_titleMeta);
    }
    if (data.containsKey('amount')) {
      context.handle(
        _amountMeta,
        amount.isAcceptableOrUnknown(data['amount']!, _amountMeta),
      );
    } else if (isInserting) {
      context.missing(_amountMeta);
    }
    if (data.containsKey('assignee')) {
      context.handle(
        _assigneeMeta,
        assignee.isAcceptableOrUnknown(data['assignee']!, _assigneeMeta),
      );
    }
    return context;
  }

  @override
  Set<GeneratedColumn> get $primaryKey => {id};
  @override
  SecretItem map(Map<String, dynamic> data, {String? tablePrefix}) {
    final effectivePrefix = tablePrefix != null ? '$tablePrefix.' : '';
    return SecretItem(
      id: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}id'],
      )!,
      userId: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}user_id'],
      ),
      updatedAt: attachedDatabase.typeMapping.read(
        DriftSqlType.dateTime,
        data['${effectivePrefix}updated_at'],
      )!,
      deleted: attachedDatabase.typeMapping.read(
        DriftSqlType.bool,
        data['${effectivePrefix}deleted'],
      )!,
      circleId: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}circle_id'],
      ),
      title: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}title'],
      )!,
      amount: attachedDatabase.typeMapping.read(
        DriftSqlType.int,
        data['${effectivePrefix}amount'],
      )!,
      assignee: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}assignee'],
      ),
      dirty: attachedDatabase.typeMapping.read(
        DriftSqlType.bool,
        data['${effectivePrefix}dirty'],
      )!,
      locked: attachedDatabase.typeMapping.read(
        DriftSqlType.bool,
        data['${effectivePrefix}locked'],
      )!,
      lockedContentEnc: attachedDatabase.typeMapping.read(
        DriftSqlType.string,
        data['${effectivePrefix}locked_content_enc'],
      ),
      lockedKeyVersion: attachedDatabase.typeMapping.read(
        DriftSqlType.int,
        data['${effectivePrefix}locked_key_version'],
      ),
    );
  }

  @override
  $SecretItemsTable createAlias(String alias) {
    return $SecretItemsTable(attachedDatabase, alias);
  }
}

class SecretItemsCompanion extends UpdateCompanion<SecretItem> {
  final Value<bool> locked;
  final Value<String?> lockedContentEnc;
  final Value<int?> lockedKeyVersion;
  final Value<String> id;
  final Value<String?> userId;
  final Value<DateTime> updatedAt;
  final Value<bool> deleted;
  final Value<bool> dirty;
  final Value<String?> circleId;
  final Value<String> title;
  final Value<int> amount;
  final Value<String?> assignee;
  final Value<int> rowid;
  const SecretItemsCompanion({
    this.locked = const Value.absent(),
    this.lockedContentEnc = const Value.absent(),
    this.lockedKeyVersion = const Value.absent(),
    this.id = const Value.absent(),
    this.userId = const Value.absent(),
    this.updatedAt = const Value.absent(),
    this.deleted = const Value.absent(),
    this.dirty = const Value.absent(),
    this.circleId = const Value.absent(),
    this.title = const Value.absent(),
    this.amount = const Value.absent(),
    this.assignee = const Value.absent(),
    this.rowid = const Value.absent(),
  });
  SecretItemsCompanion.insert({
    this.locked = const Value.absent(),
    this.lockedContentEnc = const Value.absent(),
    this.lockedKeyVersion = const Value.absent(),
    this.id = const Value.absent(),
    this.userId = const Value.absent(),
    required DateTime updatedAt,
    this.deleted = const Value.absent(),
    this.dirty = const Value.absent(),
    this.circleId = const Value.absent(),
    required String title,
    required int amount,
    this.assignee = const Value.absent(),
    this.rowid = const Value.absent(),
  }) : updatedAt = Value(updatedAt),
       title = Value(title),
       amount = Value(amount);
  static Insertable<SecretItem> custom({
    Expression<bool>? locked,
    Expression<String>? lockedContentEnc,
    Expression<int>? lockedKeyVersion,
    Expression<String>? id,
    Expression<String>? userId,
    Expression<DateTime>? updatedAt,
    Expression<bool>? deleted,
    Expression<bool>? dirty,
    Expression<String>? circleId,
    Expression<String>? title,
    Expression<int>? amount,
    Expression<String>? assignee,
    Expression<int>? rowid,
  }) {
    return RawValuesInsertable({
      if (locked != null) 'locked': locked,
      if (lockedContentEnc != null) 'locked_content_enc': lockedContentEnc,
      if (lockedKeyVersion != null) 'locked_key_version': lockedKeyVersion,
      if (id != null) 'id': id,
      if (userId != null) 'user_id': userId,
      if (updatedAt != null) 'updated_at': updatedAt,
      if (deleted != null) 'deleted': deleted,
      if (dirty != null) 'dirty': dirty,
      if (circleId != null) 'circle_id': circleId,
      if (title != null) 'title': title,
      if (amount != null) 'amount': amount,
      if (assignee != null) 'assignee': assignee,
      if (rowid != null) 'rowid': rowid,
    });
  }

  SecretItemsCompanion copyWith({
    Value<bool>? locked,
    Value<String?>? lockedContentEnc,
    Value<int?>? lockedKeyVersion,
    Value<String>? id,
    Value<String?>? userId,
    Value<DateTime>? updatedAt,
    Value<bool>? deleted,
    Value<bool>? dirty,
    Value<String?>? circleId,
    Value<String>? title,
    Value<int>? amount,
    Value<String?>? assignee,
    Value<int>? rowid,
  }) {
    return SecretItemsCompanion(
      locked: locked ?? this.locked,
      lockedContentEnc: lockedContentEnc ?? this.lockedContentEnc,
      lockedKeyVersion: lockedKeyVersion ?? this.lockedKeyVersion,
      id: id ?? this.id,
      userId: userId ?? this.userId,
      updatedAt: updatedAt ?? this.updatedAt,
      deleted: deleted ?? this.deleted,
      dirty: dirty ?? this.dirty,
      circleId: circleId ?? this.circleId,
      title: title ?? this.title,
      amount: amount ?? this.amount,
      assignee: assignee ?? this.assignee,
      rowid: rowid ?? this.rowid,
    );
  }

  @override
  Map<String, Expression> toColumns(bool nullToAbsent) {
    final map = <String, Expression>{};
    if (locked.present) {
      map['locked'] = Variable<bool>(locked.value);
    }
    if (lockedContentEnc.present) {
      map['locked_content_enc'] = Variable<String>(lockedContentEnc.value);
    }
    if (lockedKeyVersion.present) {
      map['locked_key_version'] = Variable<int>(lockedKeyVersion.value);
    }
    if (id.present) {
      map['id'] = Variable<String>(id.value);
    }
    if (userId.present) {
      map['user_id'] = Variable<String>(userId.value);
    }
    if (updatedAt.present) {
      map['updated_at'] = Variable<DateTime>(updatedAt.value);
    }
    if (deleted.present) {
      map['deleted'] = Variable<bool>(deleted.value);
    }
    if (dirty.present) {
      map['dirty'] = Variable<bool>(dirty.value);
    }
    if (circleId.present) {
      map['circle_id'] = Variable<String>(circleId.value);
    }
    if (title.present) {
      map['title'] = Variable<String>(title.value);
    }
    if (amount.present) {
      map['amount'] = Variable<int>(amount.value);
    }
    if (assignee.present) {
      map['assignee'] = Variable<String>(assignee.value);
    }
    if (rowid.present) {
      map['rowid'] = Variable<int>(rowid.value);
    }
    return map;
  }

  @override
  String toString() {
    return (StringBuffer('SecretItemsCompanion(')
          ..write('locked: $locked, ')
          ..write('lockedContentEnc: $lockedContentEnc, ')
          ..write('lockedKeyVersion: $lockedKeyVersion, ')
          ..write('id: $id, ')
          ..write('userId: $userId, ')
          ..write('updatedAt: $updatedAt, ')
          ..write('deleted: $deleted, ')
          ..write('dirty: $dirty, ')
          ..write('circleId: $circleId, ')
          ..write('title: $title, ')
          ..write('amount: $amount, ')
          ..write('assignee: $assignee, ')
          ..write('rowid: $rowid')
          ..write(')'))
        .toString();
  }
}

abstract class _$TestDatabase extends GeneratedDatabase {
  _$TestDatabase(QueryExecutor e) : super(e);
  $TestDatabaseManager get managers => $TestDatabaseManager(this);
  late final $ItemsTable items = $ItemsTable(this);
  late final $SecretItemsTable secretItems = $SecretItemsTable(this);
  @override
  Iterable<TableInfo<Table, Object?>> get allTables =>
      allSchemaEntities.whereType<TableInfo<Table, Object?>>();
  @override
  List<DatabaseSchemaEntity> get allSchemaEntities => [items, secretItems];
  @override
  DriftDatabaseOptions get options =>
      const DriftDatabaseOptions(storeDateTimeAsText: true);
}

typedef $$ItemsTableCreateCompanionBuilder =
    ItemsCompanion Function({
      Value<String> id,
      Value<String?> userId,
      required DateTime updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
      required String name,
      Value<int> rowid,
    });
typedef $$ItemsTableUpdateCompanionBuilder =
    ItemsCompanion Function({
      Value<String> id,
      Value<String?> userId,
      Value<DateTime> updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
      Value<String> name,
      Value<int> rowid,
    });

class $$ItemsTableFilterComposer extends Composer<_$TestDatabase, $ItemsTable> {
  $$ItemsTableFilterComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  ColumnFilters<String> get id => $composableBuilder(
    column: $table.id,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get userId => $composableBuilder(
    column: $table.userId,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<DateTime> get updatedAt => $composableBuilder(
    column: $table.updatedAt,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<bool> get deleted => $composableBuilder(
    column: $table.deleted,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<bool> get dirty => $composableBuilder(
    column: $table.dirty,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get name => $composableBuilder(
    column: $table.name,
    builder: (column) => ColumnFilters(column),
  );
}

class $$ItemsTableOrderingComposer
    extends Composer<_$TestDatabase, $ItemsTable> {
  $$ItemsTableOrderingComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  ColumnOrderings<String> get id => $composableBuilder(
    column: $table.id,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get userId => $composableBuilder(
    column: $table.userId,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<DateTime> get updatedAt => $composableBuilder(
    column: $table.updatedAt,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<bool> get deleted => $composableBuilder(
    column: $table.deleted,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<bool> get dirty => $composableBuilder(
    column: $table.dirty,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get name => $composableBuilder(
    column: $table.name,
    builder: (column) => ColumnOrderings(column),
  );
}

class $$ItemsTableAnnotationComposer
    extends Composer<_$TestDatabase, $ItemsTable> {
  $$ItemsTableAnnotationComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  GeneratedColumn<String> get id =>
      $composableBuilder(column: $table.id, builder: (column) => column);

  GeneratedColumn<String> get userId =>
      $composableBuilder(column: $table.userId, builder: (column) => column);

  GeneratedColumn<DateTime> get updatedAt =>
      $composableBuilder(column: $table.updatedAt, builder: (column) => column);

  GeneratedColumn<bool> get deleted =>
      $composableBuilder(column: $table.deleted, builder: (column) => column);

  GeneratedColumn<bool> get dirty =>
      $composableBuilder(column: $table.dirty, builder: (column) => column);

  GeneratedColumn<String> get name =>
      $composableBuilder(column: $table.name, builder: (column) => column);
}

class $$ItemsTableTableManager
    extends
        RootTableManager<
          _$TestDatabase,
          $ItemsTable,
          Item,
          $$ItemsTableFilterComposer,
          $$ItemsTableOrderingComposer,
          $$ItemsTableAnnotationComposer,
          $$ItemsTableCreateCompanionBuilder,
          $$ItemsTableUpdateCompanionBuilder,
          (Item, BaseReferences<_$TestDatabase, $ItemsTable, Item>),
          Item,
          PrefetchHooks Function()
        > {
  $$ItemsTableTableManager(_$TestDatabase db, $ItemsTable table)
    : super(
        TableManagerState(
          db: db,
          table: table,
          createFilteringComposer: () =>
              $$ItemsTableFilterComposer($db: db, $table: table),
          createOrderingComposer: () =>
              $$ItemsTableOrderingComposer($db: db, $table: table),
          createComputedFieldComposer: () =>
              $$ItemsTableAnnotationComposer($db: db, $table: table),
          updateCompanionCallback:
              ({
                Value<String> id = const Value.absent(),
                Value<String?> userId = const Value.absent(),
                Value<DateTime> updatedAt = const Value.absent(),
                Value<bool> deleted = const Value.absent(),
                Value<bool> dirty = const Value.absent(),
                Value<String> name = const Value.absent(),
                Value<int> rowid = const Value.absent(),
              }) => ItemsCompanion(
                id: id,
                userId: userId,
                updatedAt: updatedAt,
                deleted: deleted,
                dirty: dirty,
                name: name,
                rowid: rowid,
              ),
          createCompanionCallback:
              ({
                Value<String> id = const Value.absent(),
                Value<String?> userId = const Value.absent(),
                required DateTime updatedAt,
                Value<bool> deleted = const Value.absent(),
                Value<bool> dirty = const Value.absent(),
                required String name,
                Value<int> rowid = const Value.absent(),
              }) => ItemsCompanion.insert(
                id: id,
                userId: userId,
                updatedAt: updatedAt,
                deleted: deleted,
                dirty: dirty,
                name: name,
                rowid: rowid,
              ),
          withReferenceMapper: (p0) => p0
              .map((e) => (e.readTable(table), BaseReferences(db, table, e)))
              .toList(),
          prefetchHooksCallback: null,
        ),
      );
}

typedef $$ItemsTableProcessedTableManager =
    ProcessedTableManager<
      _$TestDatabase,
      $ItemsTable,
      Item,
      $$ItemsTableFilterComposer,
      $$ItemsTableOrderingComposer,
      $$ItemsTableAnnotationComposer,
      $$ItemsTableCreateCompanionBuilder,
      $$ItemsTableUpdateCompanionBuilder,
      (Item, BaseReferences<_$TestDatabase, $ItemsTable, Item>),
      Item,
      PrefetchHooks Function()
    >;
typedef $$SecretItemsTableCreateCompanionBuilder =
    SecretItemsCompanion Function({
      Value<bool> locked,
      Value<String?> lockedContentEnc,
      Value<int?> lockedKeyVersion,
      Value<String> id,
      Value<String?> userId,
      required DateTime updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
      Value<String?> circleId,
      required String title,
      required int amount,
      Value<String?> assignee,
      Value<int> rowid,
    });
typedef $$SecretItemsTableUpdateCompanionBuilder =
    SecretItemsCompanion Function({
      Value<bool> locked,
      Value<String?> lockedContentEnc,
      Value<int?> lockedKeyVersion,
      Value<String> id,
      Value<String?> userId,
      Value<DateTime> updatedAt,
      Value<bool> deleted,
      Value<bool> dirty,
      Value<String?> circleId,
      Value<String> title,
      Value<int> amount,
      Value<String?> assignee,
      Value<int> rowid,
    });

class $$SecretItemsTableFilterComposer
    extends Composer<_$TestDatabase, $SecretItemsTable> {
  $$SecretItemsTableFilterComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  ColumnFilters<bool> get locked => $composableBuilder(
    column: $table.locked,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get lockedContentEnc => $composableBuilder(
    column: $table.lockedContentEnc,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<int> get lockedKeyVersion => $composableBuilder(
    column: $table.lockedKeyVersion,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get id => $composableBuilder(
    column: $table.id,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get userId => $composableBuilder(
    column: $table.userId,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<DateTime> get updatedAt => $composableBuilder(
    column: $table.updatedAt,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<bool> get deleted => $composableBuilder(
    column: $table.deleted,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<bool> get dirty => $composableBuilder(
    column: $table.dirty,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get circleId => $composableBuilder(
    column: $table.circleId,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get title => $composableBuilder(
    column: $table.title,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<int> get amount => $composableBuilder(
    column: $table.amount,
    builder: (column) => ColumnFilters(column),
  );

  ColumnFilters<String> get assignee => $composableBuilder(
    column: $table.assignee,
    builder: (column) => ColumnFilters(column),
  );
}

class $$SecretItemsTableOrderingComposer
    extends Composer<_$TestDatabase, $SecretItemsTable> {
  $$SecretItemsTableOrderingComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  ColumnOrderings<bool> get locked => $composableBuilder(
    column: $table.locked,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get lockedContentEnc => $composableBuilder(
    column: $table.lockedContentEnc,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<int> get lockedKeyVersion => $composableBuilder(
    column: $table.lockedKeyVersion,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get id => $composableBuilder(
    column: $table.id,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get userId => $composableBuilder(
    column: $table.userId,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<DateTime> get updatedAt => $composableBuilder(
    column: $table.updatedAt,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<bool> get deleted => $composableBuilder(
    column: $table.deleted,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<bool> get dirty => $composableBuilder(
    column: $table.dirty,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get circleId => $composableBuilder(
    column: $table.circleId,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get title => $composableBuilder(
    column: $table.title,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<int> get amount => $composableBuilder(
    column: $table.amount,
    builder: (column) => ColumnOrderings(column),
  );

  ColumnOrderings<String> get assignee => $composableBuilder(
    column: $table.assignee,
    builder: (column) => ColumnOrderings(column),
  );
}

class $$SecretItemsTableAnnotationComposer
    extends Composer<_$TestDatabase, $SecretItemsTable> {
  $$SecretItemsTableAnnotationComposer({
    required super.$db,
    required super.$table,
    super.joinBuilder,
    super.$addJoinBuilderToRootComposer,
    super.$removeJoinBuilderFromRootComposer,
  });
  GeneratedColumn<bool> get locked =>
      $composableBuilder(column: $table.locked, builder: (column) => column);

  GeneratedColumn<String> get lockedContentEnc => $composableBuilder(
    column: $table.lockedContentEnc,
    builder: (column) => column,
  );

  GeneratedColumn<int> get lockedKeyVersion => $composableBuilder(
    column: $table.lockedKeyVersion,
    builder: (column) => column,
  );

  GeneratedColumn<String> get id =>
      $composableBuilder(column: $table.id, builder: (column) => column);

  GeneratedColumn<String> get userId =>
      $composableBuilder(column: $table.userId, builder: (column) => column);

  GeneratedColumn<DateTime> get updatedAt =>
      $composableBuilder(column: $table.updatedAt, builder: (column) => column);

  GeneratedColumn<bool> get deleted =>
      $composableBuilder(column: $table.deleted, builder: (column) => column);

  GeneratedColumn<bool> get dirty =>
      $composableBuilder(column: $table.dirty, builder: (column) => column);

  GeneratedColumn<String> get circleId =>
      $composableBuilder(column: $table.circleId, builder: (column) => column);

  GeneratedColumn<String> get title =>
      $composableBuilder(column: $table.title, builder: (column) => column);

  GeneratedColumn<int> get amount =>
      $composableBuilder(column: $table.amount, builder: (column) => column);

  GeneratedColumn<String> get assignee =>
      $composableBuilder(column: $table.assignee, builder: (column) => column);
}

class $$SecretItemsTableTableManager
    extends
        RootTableManager<
          _$TestDatabase,
          $SecretItemsTable,
          SecretItem,
          $$SecretItemsTableFilterComposer,
          $$SecretItemsTableOrderingComposer,
          $$SecretItemsTableAnnotationComposer,
          $$SecretItemsTableCreateCompanionBuilder,
          $$SecretItemsTableUpdateCompanionBuilder,
          (
            SecretItem,
            BaseReferences<_$TestDatabase, $SecretItemsTable, SecretItem>,
          ),
          SecretItem,
          PrefetchHooks Function()
        > {
  $$SecretItemsTableTableManager(_$TestDatabase db, $SecretItemsTable table)
    : super(
        TableManagerState(
          db: db,
          table: table,
          createFilteringComposer: () =>
              $$SecretItemsTableFilterComposer($db: db, $table: table),
          createOrderingComposer: () =>
              $$SecretItemsTableOrderingComposer($db: db, $table: table),
          createComputedFieldComposer: () =>
              $$SecretItemsTableAnnotationComposer($db: db, $table: table),
          updateCompanionCallback:
              ({
                Value<bool> locked = const Value.absent(),
                Value<String?> lockedContentEnc = const Value.absent(),
                Value<int?> lockedKeyVersion = const Value.absent(),
                Value<String> id = const Value.absent(),
                Value<String?> userId = const Value.absent(),
                Value<DateTime> updatedAt = const Value.absent(),
                Value<bool> deleted = const Value.absent(),
                Value<bool> dirty = const Value.absent(),
                Value<String?> circleId = const Value.absent(),
                Value<String> title = const Value.absent(),
                Value<int> amount = const Value.absent(),
                Value<String?> assignee = const Value.absent(),
                Value<int> rowid = const Value.absent(),
              }) => SecretItemsCompanion(
                locked: locked,
                lockedContentEnc: lockedContentEnc,
                lockedKeyVersion: lockedKeyVersion,
                id: id,
                userId: userId,
                updatedAt: updatedAt,
                deleted: deleted,
                dirty: dirty,
                circleId: circleId,
                title: title,
                amount: amount,
                assignee: assignee,
                rowid: rowid,
              ),
          createCompanionCallback:
              ({
                Value<bool> locked = const Value.absent(),
                Value<String?> lockedContentEnc = const Value.absent(),
                Value<int?> lockedKeyVersion = const Value.absent(),
                Value<String> id = const Value.absent(),
                Value<String?> userId = const Value.absent(),
                required DateTime updatedAt,
                Value<bool> deleted = const Value.absent(),
                Value<bool> dirty = const Value.absent(),
                Value<String?> circleId = const Value.absent(),
                required String title,
                required int amount,
                Value<String?> assignee = const Value.absent(),
                Value<int> rowid = const Value.absent(),
              }) => SecretItemsCompanion.insert(
                locked: locked,
                lockedContentEnc: lockedContentEnc,
                lockedKeyVersion: lockedKeyVersion,
                id: id,
                userId: userId,
                updatedAt: updatedAt,
                deleted: deleted,
                dirty: dirty,
                circleId: circleId,
                title: title,
                amount: amount,
                assignee: assignee,
                rowid: rowid,
              ),
          withReferenceMapper: (p0) => p0
              .map((e) => (e.readTable(table), BaseReferences(db, table, e)))
              .toList(),
          prefetchHooksCallback: null,
        ),
      );
}

typedef $$SecretItemsTableProcessedTableManager =
    ProcessedTableManager<
      _$TestDatabase,
      $SecretItemsTable,
      SecretItem,
      $$SecretItemsTableFilterComposer,
      $$SecretItemsTableOrderingComposer,
      $$SecretItemsTableAnnotationComposer,
      $$SecretItemsTableCreateCompanionBuilder,
      $$SecretItemsTableUpdateCompanionBuilder,
      (
        SecretItem,
        BaseReferences<_$TestDatabase, $SecretItemsTable, SecretItem>,
      ),
      SecretItem,
      PrefetchHooks Function()
    >;

class $TestDatabaseManager {
  final _$TestDatabase _db;
  $TestDatabaseManager(this._db);
  $$ItemsTableTableManager get items =>
      $$ItemsTableTableManager(_db, _db.items);
  $$SecretItemsTableTableManager get secretItems =>
      $$SecretItemsTableTableManager(_db, _db.secretItems);
}

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

Item _$ItemFromJson(Map<String, dynamic> json) => Item(
  id: json['id'] as String,
  userId: json['user_id'] as String?,
  updatedAt: DateTime.parse(json['updated_at'] as String),
  deleted: json['deleted'] as bool,
  name: json['name'] as String,
);

Map<String, dynamic> _$ItemToJson(Item instance) => <String, dynamic>{
  'id': instance.id,
  'user_id': instance.userId,
  'updated_at': instance.updatedAt.toIso8601String(),
  'deleted': instance.deleted,
  'name': instance.name,
};

SecretItem _$SecretItemFromJson(Map<String, dynamic> json) => SecretItem(
  id: json['id'] as String,
  userId: json['user_id'] as String?,
  updatedAt: DateTime.parse(json['updated_at'] as String),
  deleted: json['deleted'] as bool,
  circleId: json['circle_id'] as String?,
  title: json['title'] as String,
  amount: (json['amount'] as num).toInt(),
  assignee: json['assignee'] as String?,
);

Map<String, dynamic> _$SecretItemToJson(SecretItem instance) =>
    <String, dynamic>{
      'id': instance.id,
      'user_id': instance.userId,
      'updated_at': instance.updatedAt.toIso8601String(),
      'deleted': instance.deleted,
      'circle_id': instance.circleId,
      'title': instance.title,
      'amount': instance.amount,
      'assignee': instance.assignee,
    };
