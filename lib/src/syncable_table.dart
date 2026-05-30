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
  ///
  /// MIGRATION NOTE: when adding this column to a table that already holds data,
  /// the `true` default will mark every pre-existing row dirty — including rows
  /// pulled from the backend in an older app version, which would then be
  /// re-pushed on the first sync (failing RLS for rows the user doesn't own, or
  /// clobbering newer backend data). The add-column migration MUST reconcile
  /// existing rows to a correct state — either set them all to `dirty = false`
  /// (assume in sync, re-pull anything stale) or wipe and re-pull. Do not ship
  /// the column without that step.
  BoolColumn get dirty;
}
