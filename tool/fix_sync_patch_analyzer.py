from pathlib import Path

p = Path('lib/src/sync_manager.dart')
s = p.read_text()
s = s.replace('  final Duration _reconcileOverlap;\n', '', 1)
s = s.replace('       _reconcileOverlap = reconcileOverlap,\n', '', 1)
s = s.replace(
    "       assert(\n         syncInterval.inMilliseconds > 0,\n         'Sync interval must be positive',\n       ) {",
    "       assert(\n         syncInterval.inMilliseconds > 0,\n         'Sync interval must be positive',\n       ),\n       assert(!reconcileOverlap.isNegative, 'Reconcile overlap cannot be negative') {",
    1,
)
s = s.replace('Error.throwWithStackTrace(firstError!, firstStack!);', 'Error.throwWithStackTrace(firstError, firstStack!);')
s = s.replace(
    "  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(\n    Type syncable, {\n    bool fullResync = false,\n  }) async {",
    "  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(\n    Type syncable,\n  ) async {",
    1,
)
p.write_text(s)

t = Path('test/sync_manager_test.dart')
ts = t.read_text()
ts = ts.replace("import 'package:logging/logging.dart';\n", '', 1)
start = ts.find('class LocalReturningTimestampStorage extends TimestampStorage {')
end = ts.find('class FixedRandom implements Random {', start)
if start >= 0 and end >= 0:
    # Remove the now-obsolete comment and class together.
    comment = ts.rfind('///', 0, start)
    if comment >= 0 and ts[comment:start].strip().startswith('///'):
        start = comment
    ts = ts[:start] + ts[end:]
needle = "    final retryTimer = timerFactory.timers.firstWhere((timer) => timer.isActive);\n    retryTimer.fire();"
ts = ts.replace(
    needle,
    "    final retryTimer = timerFactory.timers.firstWhere((timer) => timer.isActive);\n    expect(retryTimer.duration, const Duration(seconds: 5));\n    retryTimer.fire();",
    1,
)
t.write_text(ts)
