import 'package:drift/drift.dart';

/// A Drift table that can be synchronized with a backend.
abstract class SyncableTable implements Table {
  TextColumn get id;
  TextColumn get userId;
  DateTimeColumn get updatedAt;
  BoolColumn get deleted;

  /// Local-only push-tracking flag. `true` = has unpushed local changes.
  /// Implementers should default it to `true` so a freshly-inserted local row
  /// is pushed, e.g.:
  ///
  /// ```dart
  /// @override
  /// BoolColumn get dirty => boolean().withDefault(const Constant(true))();
  /// ```
  BoolColumn get dirty;
}
