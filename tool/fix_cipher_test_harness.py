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
    """    // Current PostgREST executes requests through BaseClient.send(). Preserve\n    // the suite's original semantics: GETs read [backendRows], POSTs record the\n    // exact upsert payload and echo it back as a successful response.\n    when(mockHttpClient.send(any)).thenAnswer((invocation) async {\n      final request = invocation.positionalArguments[0] as BaseRequest;\n      if (request.method == 'POST') {\n        final body = (request as Request).body;\n        pushedBatches.add(\n          (jsonDecode(body) as List).cast<Map<String, dynamic>>(),\n        );\n        return StreamedResponse(\n          Stream<List<int>>.value(utf8.encode(body)),\n          200,\n          request: request,\n          headers: {'content-type': 'application/json; charset=utf-8'},\n        );\n      }\n\n      final bytes = utf8.encode(jsonEncode(backendRows));\n      return StreamedResponse(\n        Stream<List<int>>.value(bytes),\n        200,\n        request: request,\n        headers: {'content-type': 'application/json; charset=utf-8'},\n      );\n    });\n""",
    1,
)

slow_post_anchor = """    when(\n      mockHttpClient.post(\n        any,\n        headers: anyNamed('headers'),\n        body: anyNamed('body'),\n      ),\n    ).thenAnswer((inv) async {\n      final body = inv.namedArguments[#body] as String;\n      final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();\n      if (rows.isNotEmpty && rows.first.containsKey(nameKey)) {\n        itemsPushStarted = true;\n        await Future<void>.delayed(const Duration(milliseconds: 250));\n      } else {\n        pushedBatches.add(rows);\n      }\n      return Response(\n        body,\n        200,\n        request: Request('POST', Uri()),\n        headers: {'content-type': 'application/json; charset=utf-8'},\n      );\n    });\n"""
if slow_post_anchor not in s:
    raise SystemExit('slow POST harness anchor not found')
s = s.replace(
    slow_post_anchor,
    """    when(mockHttpClient.send(any)).thenAnswer((invocation) async {\n      final request = invocation.positionalArguments[0] as BaseRequest;\n      if (request.method == 'POST') {\n        final body = (request as Request).body;\n        final rows = (jsonDecode(body) as List).cast<Map<String, dynamic>>();\n        if (rows.isNotEmpty && rows.first.containsKey(nameKey)) {\n          itemsPushStarted = true;\n          await Future<void>.delayed(const Duration(milliseconds: 250));\n        } else {\n          pushedBatches.add(rows);\n        }\n        return StreamedResponse(\n          Stream<List<int>>.value(utf8.encode(body)),\n          200,\n          request: request,\n          headers: {'content-type': 'application/json; charset=utf-8'},\n        );\n      }\n\n      return StreamedResponse(\n        Stream<List<int>>.value(utf8.encode(jsonEncode(backendRows))),\n        200,\n        request: request,\n        headers: {'content-type': 'application/json; charset=utf-8'},\n      );\n    });\n""",
    1,
)

reject_anchor = """      when(\n        mockHttpClient.post(\n          any,\n          headers: anyNamed('headers'),\n          body: anyNamed('body'),\n        ),\n      ).thenAnswer((inv) async {\n        final body = inv.namedArguments[#body] as String;\n        if (failuresLeft > 0) {\n          failuresLeft--;\n          return Response(\n            jsonEncode({\n              'code': '42501',\n              'message': 'E2E_PLAINTEXT_REJECTED: stale mode',\n              'details': 'Forbidden',\n              'hint': null,\n            }),\n            403,\n            request: Request('POST', Uri()),\n            headers: {'content-type': 'application/json; charset=utf-8'},\n          );\n        }\n        pushedBatches.add(\n          (jsonDecode(body) as List).cast<Map<String, dynamic>>(),\n        );\n        return Response(\n          body,\n          200,\n          request: Request('POST', Uri()),\n          headers: {'content-type': 'application/json; charset=utf-8'},\n        );\n      });\n"""
if reject_anchor not in s:
    raise SystemExit('quarantine rejection harness anchor not found')
s = s.replace(
    reject_anchor,
    """      when(mockHttpClient.send(any)).thenAnswer((invocation) async {\n        final request = invocation.positionalArguments[0] as BaseRequest;\n        if (request.method == 'POST') {\n          final body = (request as Request).body;\n          if (failuresLeft > 0) {\n            failuresLeft--;\n            final errorBody = jsonEncode({\n              'code': '42501',\n              'message': 'E2E_PLAINTEXT_REJECTED: stale mode',\n              'details': 'Forbidden',\n              'hint': null,\n            });\n            return StreamedResponse(\n              Stream<List<int>>.value(utf8.encode(errorBody)),\n              403,\n              request: request,\n              headers: {'content-type': 'application/json; charset=utf-8'},\n            );\n          }\n          pushedBatches.add(\n            (jsonDecode(body) as List).cast<Map<String, dynamic>>(),\n          );\n          return StreamedResponse(\n            Stream<List<int>>.value(utf8.encode(body)),\n            200,\n            request: request,\n            headers: {'content-type': 'application/json; charset=utf-8'},\n          );\n        }\n\n        return StreamedResponse(\n          Stream<List<int>>.value(utf8.encode(jsonEncode(backendRows))),\n          200,\n          request: request,\n          headers: {'content-type': 'application/json; charset=utf-8'},\n        );\n      });\n""",
    1,
)

p.write_text(s)
