# Grok capture fixtures

These are focused derivatives of the two Grok 1.0.89 requests in the FLT-113
stage (a) smoke, supplied by the parent on Alien for FLT-189. The parent stripped
body strings except protocol enums and models, kept an allowlist of headers and
replaced UUIDs consistently before transfer to Anton. Original transcripts,
credentials and system prompts are not included.

Parent derivative SHA-256 checksums:

- Main: `365322d6fa21b043d91abc3a7ecfadc45382128e04c722bed722ad435ed1f80e`
- Helper: `904e8e2924fa3c1331f3ce637ac204dd5b179d28830f88c8d50ea569871aeb8e`

The focused files keep the methods, paths, body keys, models and relevant header
relationships. Tools are reduced to empty arrays; unrelated client headers are
removed. Main retains its five message roles with stripped content, except for
one synthetic search marker. Helper retains its two input message roles with
stripped content. The main request remains larger than the helper, as in the
source capture, so analytics selects its conversation through the existing rule.

Main's session and conversation IDs both matched the native ACP session (here
`00000000-0000-0000-0000-000000000001`). Its group ID differs (here
`00000000-0000-0000-0000-000000000002`). Helper's session, conversation, agent and
request headers are present but empty; only its group matches main. No helper
native session identity is asserted. Agent and request IDs are not session keys.

Capture records main's native session separately from the shared group. Existing
explicit metadata wins. The existing analytics model groups by client session,
retains native session IDs as alternates and links each usage row by trace.
