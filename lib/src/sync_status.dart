/// An immutable snapshot of the sync engine's current work.
///
/// Pending counts include work in flight, rejected rows and changes waiting for
/// encryption keys. An empty queue alone never proves synchronization succeeded.
class SyncStatus {
  const SyncStatus({
    this.enabled = false,
    this.checking = false,
    this.downloading = false,
    this.uploading = false,
    this.pendingUploads = 0,
    this.pendingDownloads = 0,
    this.failedUploads = 0,
    this.failedDownloads = 0,
    this.deferredUploads = 0,
    this.retryScheduled = false,
    this.checkFailed = false,
    this.lastCheckedAt,
  });

  final bool enabled;
  final bool checking;
  final bool downloading;
  final bool uploading;
  final int pendingUploads;
  final int pendingDownloads;
  final int failedUploads;
  final int failedDownloads;
  final int deferredUploads;
  final bool retryScheduled;
  final bool checkFailed;

  /// Completion time of the last successful check of every registered table.
  /// This is not a claim that pending uploads or rejected rows have synced.
  final DateTime? lastCheckedAt;

  bool get isBusy => checking || downloading || uploading;
  bool get hasFailures =>
      checkFailed || failedUploads > 0 || failedDownloads > 0;
  bool get hasPendingChanges => pendingUploads > 0 || pendingDownloads > 0;

  Object get _values => (
    enabled,
    checking,
    downloading,
    uploading,
    pendingUploads,
    pendingDownloads,
    failedUploads,
    failedDownloads,
    deferredUploads,
    retryScheduled,
    checkFailed,
    lastCheckedAt,
  );

  @override
  bool operator ==(Object other) =>
      other is SyncStatus && _values == other._values;

  @override
  int get hashCode => _values.hashCode;
}
