import 'dart:async';

import 'package:drift/drift.dart';
import 'package:syncable/src/syncable.dart';
import 'package:syncable/src/syncable_table.dart';

/// A mixin to enable synchronization with a backend via a [SyncManager].
mixin SyncableDatabase on GeneratedDatabase {
  final Map<
    (
      TableInfo<SyncableTable, Syncable>,
      Expression<bool> Function(SyncableTable),
    ),
    Stream<List<Syncable>>
  >
  _queryStreams = {};

  /// Retrieves a single item by its [id] from the local database [table].
  Future<T> getItem<T extends Syncable>(
    TableInfo<SyncableTable, T> table,
    String id,
  ) async {
    return await (select(table)..where((row) => row.id.equals(id))).getSingle();
  }

  /// Deletes all data from all tables.
  Future<void> clear() async {
    await transaction(() async {
      for (final table in allTables) {
        await delete(table).go();
      }
    });
  }

  /// Subscribes to [table] and listens for rows matching [filter].
  ///
  /// Drift emits the complete result set whenever the query is invalidated, so
  /// callers should make [filter] as selective as possible. SyncManager uses
  /// [subscribeToDirty] for outgoing discovery so a one-row edit does not
  /// materialize the table's accumulated history.
  StreamSubscription<List<T>> subscribe<T extends Syncable>({
    required TableInfo<SyncableTable, T> table,
    required Expression<bool> Function(SyncableTable) filter,
    required void Function(List<T>) onChange,
  }) {
    if (!_queryStreams.containsKey((table, filter))) {
      final query = (select(table)
        ..where(filter)
        ..orderBy([(row) => OrderingTerm.asc(row.updatedAt)]));

      _queryStreams[(table, filter)] = query.watch();
    }

    final stream = _queryStreams[(table, filter)]! as Stream<List<T>>;

    return stream.listen(onChange);
  }

  /// Watches only rows whose persistent dirty flag is set.
  ///
  /// This is the normal outgoing-sync subscription. Filtering in SQL avoids
  /// repeatedly materializing clean history merely to discard it in Dart.
  StreamSubscription<List<T>> subscribeToDirty<T extends Syncable>({
    required TableInfo<SyncableTable, T> table,
    required void Function(List<T>) onChange,
  }) {
    return subscribe(
      table: table,
      filter: (row) => row.dirty.equals(true),
      onChange: onChange,
    );
  }

  /// Retrieves the table for a specific syncable type [T].
  TableInfo<SyncableTable, T> getTable<T extends Syncable>() {
    return allTables.whereType<TableInfo<SyncableTable, T>>().first;
  }
}
