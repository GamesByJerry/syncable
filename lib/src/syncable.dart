import 'package:drift/drift.dart';

/// An entity that can be synchronized between a local database and the backend.
abstract class Syncable {
  String get id;
  String? get userId;
  DateTime get updatedAt;
  bool get deleted;

  /// Whether this row has local changes that still need to be pushed to the
  /// backend. Set `true` on a local create/update; the sync engine clears it to
  /// `false` once the row is pulled from, or successfully pushed to, the
  /// backend. Persisting this (it is a real column — see [SyncableTable.dirty])
  /// is what stops a pulled-but-unmodified row from being re-pushed after an
  /// app restart (which would otherwise fail RLS for rows the user doesn't own,
  /// or clobber a newer backend version).
  bool get dirty;

  /// Converts this object to a JSON representation to send it to the backend.
  ///
  /// MUST NOT include the local-only [dirty] column — the backend tables have
  /// no such column.
  Map<String, dynamic> toJson();

  /// Converts this object to a Drift companion object to write to the local
  /// database.
  UpdateCompanion<Syncable> toCompanion();
}
