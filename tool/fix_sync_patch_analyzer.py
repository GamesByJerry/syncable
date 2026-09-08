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
s = s.replace(
    'Error.throwWithStackTrace(firstError!, firstStack!);',
    'Error.throwWithStackTrace(firstError, firstStack!);',
)
s = s.replace(
    "  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(\n    Type syncable, {\n    bool fullResync = false,\n  }) async {",
    "  Future<List<Map<String, dynamic>>> _fetchBackendItemMetadata(\n    Type syncable,\n  ) async {",
    1,
)

# Dirty is authoritative for discovery, but the same dirty version must not be
# re-enqueued while its first push is still awaiting the backend. The old
# table-wide watermark accidentally provided this suppression while also being
# able to strand older dirty rows. Replace it with per-row version state:
# successful versions live in _sentItems and currently-attempted versions live
# in _outgoingInFlight.
field_anchor = "  final Map<Type, Map<String, DateTime>> _outgoingRetryAttempts = {};\n"
if field_anchor not in s:
    raise SystemExit('outgoing retry field anchor not found')
s = s.replace(
    field_anchor,
    field_anchor + "  final Map<Type, Set<Syncable>> _outgoingInFlight = {};\n",
    1,
)

init_anchor = "    _outQueues[S] = {};\n"
if init_anchor not in s:
    raise SystemExit('out queue initialization anchor not found')
s = s.replace(
    init_anchor,
    init_anchor + "    _outgoingInFlight[S] = {};\n",
    1,
)

predicate_old = """    bool updateHasNotBeenSentYet(Syncable row) =>
        row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0));
"""
predicate_new = """    bool updateHasNotBeenSentYet(Syncable row) {
      if (_sentItems[syncable]!.contains(row) ||
          _outgoingInFlight[syncable]!.contains(row)) {
        return false;
      }
      return row.updatedAt.isAfter(outQueue[row.id]?.updatedAt ?? DateTime(0));
    }
"""
if predicate_old not in s:
    raise SystemExit('outgoing discovery predicate anchor not found')
s = s.replace(predicate_old, predicate_new, 1)

# Hold the selected versions in the per-row in-flight set across encoding and
# the network verdict. A finally block guarantees exceptions do not strand an
# in-flight marker. Successful versions remain suppressed by _sentItems; failed
# versions are requeued and become eligible again after this marker is released.
process_start = s.find("    assert(!outgoing.any((row) => row.userId?.isEmpty ?? true));")
process_end_marker = "\n  }\n\n  Future<void> _upsertPayloads("
process_end = s.find(process_end_marker, process_start)
if process_start < 0 or process_end < 0:
    raise SystemExit('process outgoing wrapping anchors not found')
body = s[process_start:process_end]
indented = '\n'.join('  ' + line if line else line for line in body.split('\n'))
wrapped = (
    "    final inFlight = _outgoingInFlight[syncable]!;\n"
    "    inFlight.addAll(outgoing);\n"
    "    try {\n"
    f"{indented}\n"
    "    } finally {\n"
    "      inFlight.removeAll(outgoing);\n"
    "    }"
)
s = s[:process_start] + wrapped + s[process_end:]

p.write_text(s)

t = Path('test/sync_manager_test.dart')
ts = t.read_text()
ts = ts.replace("import 'package:logging/logging.dart';\n", '', 1)
start = ts.find('class LocalReturningTimestampStorage extends TimestampStorage {')
end = ts.find('class FixedRandom implements Random {', start)
if start >= 0 and end >= 0:
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
