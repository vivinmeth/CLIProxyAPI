package archive

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

func TestCodexSkillsAndThreadContext(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	bodies := []string{
		`{"role":"user","content":"ship the fix"}`,
		`{"role":"user","content":"Tool loaded.\nAvailable tools follow."}`,
		`{"type":"reasoning","summary":[{"text":"read the release rules first"}]}`,
		`{"type":"custom_tool_call","name":"exec","input":"const r = await tools.exec_command({cmd: 'cat /h/skills/full-send/SKILL.md'}); text(r);"}`,
		`{"type":"custom_tool_call","name":"js","input":"await tools.exec_command({cmd: 'cat /h/skills/go/SKILL.md'})"}`,
		`{"type":"function_call","name":"exec_command","arguments":"{\"cmd\":\"cat /h/skills/environ/SKILL.md\"}"}`,
		`{"type":"custom_tool_call","name":"exec","input":"await tools.exec_command({cmd: 'cat > /h/skills/created/SKILL.md'})"}`,
		`{"type":"custom_tool_call","name":"js","input":"await tools.exec_command({cmd: 'tee /h/skills/edited/SKILL.md'})"}`,
	}
	var msgs []MessageIngest
	for i, body := range bodies {
		msgs = append(msgs, MessageIngest{ExternalID: string(rune('a' + i)), Seq: int64(i), Message: json.RawMessage(body)})
	}
	if _, err := a.IngestSession(ctx, SessionIngest{Harness: "codex", ExternalID: "skill-session", Messages: msgs}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.IngestThreads(ctx, ThreadBatch{Source: "t3", Machine: "alien", Threads: []ThreadIngest{
		{ExternalID: "skill-thread", Sessions: []ThreadSession{{Harness: "codex", SessionID: "skill-session", Role: "main"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM skill_invocations k JOIN sessions s ON s.id=k.session_id
		JOIN session_threads st ON st.session_id=s.id JOIN threads t ON t.id=st.thread_id
		WHERE t.id='t3:skill-thread' AND s.harness='codex' AND k.via='file'`); n != 3 {
		t.Fatalf("Codex skills linked to thread = %d, want 3", n)
	}
	var prompt, reasoning string
	if err := an.db.QueryRow(`SELECT u.text, r.text FROM skill_invocations k
		LEFT JOIN LATERAL (SELECT * FROM user_prompts u WHERE u.session_id=k.session_id
			AND (u.seq,u.idx)<(k.seq,k.idx) ORDER BY u.seq DESC,u.idx DESC LIMIT 1) u ON true
		LEFT JOIN LATERAL (SELECT text FROM items r WHERE r.session_id=k.session_id AND r.kind='thinking'
			AND r.role='assistant' AND (r.seq,r.idx)<(k.seq,k.idx) AND (r.seq,r.idx)>(u.seq,u.idx)
			ORDER BY r.seq DESC,r.idx DESC LIMIT 1) r ON true
		WHERE k.skill='full-send'`).Scan(&prompt, &reasoning); err != nil || prompt != "ship the fix" || reasoning != "read the release rules first" {
		t.Fatalf("context = %q / %q (%v)", prompt, reasoning, err)
	}
}

func TestProxyAttributionAfterRequestArrives(t *testing.T) {
	_, an := openTestAnalytics(t)
	drain(t, an)
	if _, err := an.db.Exec(`INSERT INTO sessions(id,kind,trace_id,version) VALUES
		('proxy:late','proxy','late-trace',2), ('proxy:conflict','proxy','conflict-trace',2),
		('proxy:unknown','proxy','',2), ('proxy:old','proxy','old-trace',2)`); err != nil {
		t.Fatal(err)
	}
	rl := requestlog.Default()
	if rl == nil {
		t.Fatal("request log missing")
	}
	if _, err := rl.DB().Exec(`INSERT INTO ` + rl.Schema() + `.requests(ts,trace_id,harness,machine,outcome) VALUES
		(now(),'late-trace','codex','alien','success'), (now(),'conflict-trace','codex','alien','success'),
		(now(),'conflict-trace','claude-code','antonmk3','success'), (now(),'','claude-code','alien','success')`); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id='proxy:late' AND harness='codex' AND machine='alien'`); n != 1 {
		t.Fatal("late request labels were not filled")
	}
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id IN ('proxy:conflict','proxy:unknown') AND harness='' AND machine=''`); n != 2 {
		t.Fatal("ambiguous or empty trace guessed labels")
	}
	// Startup repairs old extractions; existing labels survive a conflicting request label.
	if _, err := an.db.Exec(`INSERT INTO requests(id,ts,trace_id,harness,machine) VALUES
		(-1,now(),'old-trace','claude-code','antonmk3');
		UPDATE sessions SET machine='alien' WHERE id='proxy:old'`); err != nil {
		t.Fatal(err)
	}
	an.refreshLinks(true)
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id='proxy:old' AND harness='claude-code' AND machine='alien'`); n != 1 {
		t.Fatal("startup repair failed or overwrote an existing label")
	}
}
