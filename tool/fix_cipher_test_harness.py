from pathlib import Path

p = Path('test/sync_field_cipher_seam_test.dart')
s = p.read_text()

session_anchor = """    when(mockSupabaseClient.auth).thenReturn(mockGoTrue);\n    when(mockGoTrue.currentSession).thenReturn(mockSession);\n    when(mockGoTrue.onAuthStateChange).thenAnswer((_) => authEvents.stream);\n"""
if session_anchor not in s:
    raise SystemExit('session harness anchor not found')
s = s.replace(
    session_anchor,
    """    when(mockSupabaseClient.auth).thenReturn(mockGoTrue);\n    when(mockGoTrue.currentSession).thenReturn(mockSession);\n    // Session is deliberately live in this suite. Mockito's generated fallback\n    // for an unstubbed bool getter is false today, but make the contract explicit\n    // so auth-gate behavior cannot silently change with mock generation.\n    when(mockSession.isExpired).thenReturn(false);\n    when(mockGoTrue.onAuthStateChange).thenAnswer((_) => authEvents.stream);\n""",
    1,
)

get_anchor = """    // Serve [backendRows] to both the id/updated_at metadata sweep and the\n    // full-row batch pull (the sweep only reads the id/updated_at keys).\n    when(mockHttpClient.get(any, headers: anyNamed('headers'))).thenAnswer(\n      (_) async => Response(\n        jsonEncode(backendRows),\n        200,\n        request: Request('GET', Uri()),\n      ),\n    );\n"""
if get_anchor not in s:
    raise SystemExit('GET harness anchor not found')
s = s.replace(
    get_anchor,
    """    // PostgREST executes SELECTs through BaseClient.send(), not get().\n    // Serve [backendRows] to both metadata discovery and full-row batch pulls.\n    when(mockHttpClient.send(any)).thenAnswer((invocation) async {\n      final request = invocation.positionalArguments[0] as BaseRequest;\n      final bytes = utf8.encode(jsonEncode(backendRows));\n      return StreamedResponse(\n        Stream<List<int>>.value(bytes),\n        200,\n        request: request,\n        headers: {'content-type': 'application/json; charset=utf-8'},\n      );\n    });\n""",
    1,
)

p.write_text(s)
