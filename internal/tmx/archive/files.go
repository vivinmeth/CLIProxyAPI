package archive

// Session files (tokenmaxx fork): a raw transcript that only grows can be uploaded as just its
// new bytes. Each append is stored as its own content-addressed chunk blob; existing objects are
// never rewritten. The logical file is the ordered chunks of its current generation:
//
//   - a full upload (PUT /tmx/archive/sessions with the file in sources) newer than the chain
//     starts the next generation with that blob as chunk 0, so whole-file uploads and appends
//     coexist; the chain is materialised lazily on the first append after it;
//   - an append with reset=true starts the next generation with the bytes sent (the client's
//     fallback when its view and the server's differ);
//   - every chunk row keeps the running sha256 of the file so far and the hash state, so an
//     append verifies and extends the file hash without reading earlier chunks.
//
// Chunk rows are append-only and survive session delete (old generations stay readable).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"
)

// FileState is what the server has of a session file.
type FileState struct {
	SessionID  string    `json:"session_id"` // <harness>:<external_id>
	Name       string    `json:"name"`
	Length     int64     `json:"length"`
	SHA256     string    `json:"sha256"`     // of the whole logical file
	Generation int       `json:"generation"` // bumps on a full upload or reset
	Chunks     int       `json:"chunks"`     // chunk objects that make up the file
	Origin     string    `json:"origin"`     // chunks | source (a full upload not appended to yet)
	UpdatedAt  time.Time `json:"updated_at"`
}

// FileAppend is one item of POST /tmx/archive/files/append.
type FileAppend struct {
	Harness   string `json:"harness"`
	SessionID string `json:"session_id"` // the harness's session id (as imported)
	Name      string `json:"name"`       // the source name, as in sessions[].sources[].name
	// Offset is where the bytes go: must equal the server's current length (0 for a new file
	// or a reset).
	Offset int64 `json:"offset"`
	// BaseSHA256 (optional) is the sha256 of the first Offset bytes as the client has them;
	// checked against the server's file hash.
	BaseSHA256 string `json:"base_sha256,omitempty"`
	// The new bytes: inline, or a blob uploaded first with PUT /tmx/archive/blobs/<sha256>.
	DataBase64 string `json:"data_base64,omitempty"`
	Blob       string `json:"blob,omitempty"`
	// SHA256 (optional) is the expected sha256 of the whole file after the append.
	SHA256 string `json:"sha256,omitempty"`
	// Reset makes the bytes the whole file (a new generation); Offset must be 0.
	Reset bool `json:"reset,omitempty"`
}

// FileAppendBatch is the body of POST /tmx/archive/files/append.
type FileAppendBatch struct {
	Appends []FileAppend `json:"appends"`
}

// FileRef names a session file.
type FileRef struct {
	Harness   string `json:"harness"`
	SessionID string `json:"session_id"`
	Name      string `json:"name"`
}

const (
	maxFileAppendBatch   = 100
	maxFileAppendInline  = 32 << 20
	maxFileAppendBytes   = 512 << 20
	maxFileNameLength    = 1024
	maxFileStateBatch    = 1000
	fileOriginChunks     = "chunks"
	fileOriginSource     = "source"
	emptySHA256          = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	chunkOriginSource    = "source" // chunk 0 copied from a full upload
	chunkOriginAppend    = "append"
	chunkOriginReset     = "reset"
	errFileNameRequired  = "name is required (at most 1024 characters)"
	errFileSessionNeeded = "harness and session_id are required (no spaces)"
)

var (
	// ErrFileNotFound is returned when the server has nothing for a session file.
	ErrFileNotFound = errors.New("file not found")
	// ErrBadFileRef wraps an invalid harness / session_id / name.
	ErrBadFileRef = errors.New("bad file reference")
)

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// fileView is the resolved state plus what an append needs to extend it.
type fileView struct {
	state     *FileState // nil: nothing stored
	tailGen   int        // highest chain generation (0: no chain)
	tailSeq   int
	hashState []byte // chain tail's sha256 state (state.Origin == chunks)
	source    string // state.Origin == source: the full upload's blob
}

func (a *Archive) resolveFile(ctx context.Context, q querier, sessionID, name string) (*fileView, error) {
	v := &fileView{}
	var tail struct {
		gen, seq   int
		end        int64
		sha        string
		state      []byte
		at, genAt  time.Time
		chunkCount int
	}
	err := q.QueryRowContext(ctx, `SELECT c.generation, c.seq, c."offset" + c.length, c.file_sha256, c.hash_state,
			c.created_at, g.created_at, (SELECT count(*) FROM `+schema+`.session_file_chunks x
				WHERE x.session_id = c.session_id AND x.name = c.name AND x.generation = c.generation)
		FROM `+schema+`.session_file_chunks c JOIN `+schema+`.session_file_chunks g
			ON g.session_id = c.session_id AND g.name = c.name AND g.generation = c.generation AND g.seq = 0
		WHERE c.session_id = $1 AND c.name = $2 ORDER BY c.generation DESC, c.seq DESC LIMIT 1`,
		sessionID, name).Scan(&tail.gen, &tail.seq, &tail.end, &tail.sha, &tail.state, &tail.at, &tail.genAt, &tail.chunkCount)
	hasChain := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var src struct {
		blob string
		size int64
		at   time.Time
	}
	err = q.QueryRowContext(ctx, `SELECT blob, size, ingested_at FROM `+schema+`.session_sources
		WHERE session_id = $1 AND name = $2 ORDER BY ingested_at DESC, blob DESC LIMIT 1`,
		sessionID, name).Scan(&src.blob, &src.size, &src.at)
	hasSource := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if hasChain {
		v.tailGen, v.tailSeq = tail.gen, tail.seq
	}
	switch {
	case hasSource && (!hasChain || src.at.After(tail.genAt)):
		// A full upload newer than the chain is the file now.
		v.source = src.blob
		v.state = &FileState{SessionID: sessionID, Name: name, Length: src.size, SHA256: src.blob,
			Generation: v.tailGen + 1, Chunks: 1, Origin: fileOriginSource, UpdatedAt: src.at}
	case hasChain:
		v.hashState = tail.state
		v.state = &FileState{SessionID: sessionID, Name: name, Length: tail.end, SHA256: tail.sha,
			Generation: tail.gen, Chunks: tail.chunkCount, Origin: fileOriginChunks, UpdatedAt: tail.at}
	}
	return v, nil
}

func fileSessionKey(harness, sessionID string) (string, bool) {
	harness, sessionID = strings.TrimSpace(harness), strings.TrimSpace(sessionID)
	if harness == "" {
		// "<harness>:<external_id>" in one field
		h, ext, ok := strings.Cut(sessionID, ":")
		if !ok {
			return "", false
		}
		harness, sessionID = h, ext
	}
	if !validIdent(harness, 64) || !validIdent(sessionID, 256) {
		return "", false
	}
	return SessionKey(harness, sessionID), true
}

func validFileName(name string) bool {
	return strings.TrimSpace(name) != "" && len(name) <= maxFileNameLength
}

// FileStateOf returns the server's state of one session file (ErrFileNotFound when none).
func (a *Archive) FileStateOf(ctx context.Context, ref FileRef) (*FileState, error) {
	key, ok := fileSessionKey(ref.Harness, ref.SessionID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrBadFileRef, errFileSessionNeeded)
	}
	if !validFileName(ref.Name) {
		return nil, fmt.Errorf("%w: %s", ErrBadFileRef, errFileNameRequired)
	}
	v, err := a.resolveFile(ctx, a.db, key, ref.Name)
	if err != nil {
		return nil, err
	}
	if v.state == nil {
		return nil, ErrFileNotFound
	}
	return v.state, nil
}

// FileStates is the batch form of FileStateOf: status found | not_found | error per item.
func (a *Archive) FileStates(ctx context.Context, refs []FileRef) (*BatchResult, error) {
	if len(refs) > maxFileStateBatch {
		return nil, fmt.Errorf("at most %d files per batch", maxFileStateBatch)
	}
	res := &BatchResult{Results: make([]ItemResult, 0, len(refs))}
	for i, ref := range refs {
		st, err := a.FileStateOf(ctx, ref)
		switch {
		case errors.Is(err, ErrFileNotFound):
			res.Results = append(res.Results, ItemResult{Index: i, Status: "not_found", Code: 404})
		case err != nil:
			code := 500
			if errors.Is(err, ErrBadFileRef) {
				code = 400
			}
			it := itemError(i, code, err.Error())
			res.Errors++
			res.Results = append(res.Results, it)
		default:
			res.Exists++
			res.Results = append(res.Results, ItemResult{Index: i, Status: "found", Code: 200, File: st})
		}
	}
	return res, nil
}

// ListFiles returns the state of every file of a session (uploaded whole or appended).
func (a *Archive) ListFiles(ctx context.Context, sessionKey string) ([]FileState, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT name FROM `+schema+`.session_sources WHERE session_id = $1
		UNION SELECT name FROM `+schema+`.session_file_chunks WHERE session_id = $1 ORDER BY 1`, sessionKey)
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var n string
		if err = rows.Scan(&n); err != nil {
			_ = rows.Close()
			return nil, err
		}
		names = append(names, n)
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	out := []FileState{}
	for _, n := range names {
		v, errV := a.resolveFile(ctx, a.db, sessionKey, n)
		if errV != nil {
			return nil, errV
		}
		if v.state != nil {
			out = append(out, *v.state)
		}
	}
	return out, nil
}

// AppendFiles applies a batch of appends, each in its own transaction, with per-item results.
func (a *Archive) AppendFiles(ctx context.Context, in FileAppendBatch) (*BatchResult, error) {
	if len(in.Appends) > maxFileAppendBatch {
		return nil, fmt.Errorf("at most %d appends per batch", maxFileAppendBatch)
	}
	var inline int
	for _, it := range in.Appends {
		inline += base64.StdEncoding.DecodedLen(len(it.DataBase64))
	}
	if inline > 2*maxFileAppendInline {
		return nil, fmt.Errorf("at most %d inline bytes per batch; upload big appends as blobs", 2*maxFileAppendInline)
	}
	res := &BatchResult{Results: make([]ItemResult, 0, len(in.Appends))}
	for i, it := range in.Appends {
		res.add(a.appendFile(ctx, i, it))
	}
	return res, nil
}

func (a *Archive) appendFile(ctx context.Context, i int, in FileAppend) ItemResult {
	key, ok := fileSessionKey(in.Harness, in.SessionID)
	if !ok || strings.TrimSpace(in.Harness) == "" {
		return itemError(i, 400, errFileSessionNeeded)
	}
	if !validFileName(in.Name) {
		return itemError(i, 400, errFileNameRequired)
	}
	in.BaseSHA256, in.SHA256 = strings.ToLower(strings.TrimSpace(in.BaseSHA256)), strings.ToLower(strings.TrimSpace(in.SHA256))
	in.Blob = strings.ToLower(strings.TrimSpace(in.Blob))
	switch {
	case in.Offset < 0:
		return itemError(i, 400, "offset can't be negative")
	case in.Reset && in.Offset != 0:
		return itemError(i, 400, "reset needs offset 0")
	case (in.DataBase64 == "") == (in.Blob == ""):
		return itemError(i, 400, "send data_base64 or blob (exactly one)")
	case in.BaseSHA256 != "" && !ValidHash(in.BaseSHA256), in.SHA256 != "" && !ValidHash(in.SHA256),
		in.Blob != "" && !ValidHash(in.Blob):
		return itemError(i, 400, "sha256 values must be 64 hex characters")
	}
	var data []byte
	if in.DataBase64 != "" {
		var err error
		if data, err = base64.StdEncoding.DecodeString(in.DataBase64); err != nil {
			return itemError(i, 400, "data_base64: "+err.Error())
		}
		if len(data) > maxFileAppendInline {
			return itemError(i, 413, fmt.Sprintf("inline data larger than %d bytes; upload it as a blob", maxFileAppendInline))
		}
	} else {
		exists, err := a.blobs.lookup(ctx, in.Blob)
		if err != nil {
			return itemError(i, 500, "blob lookup: "+err.Error())
		}
		if !exists {
			return ItemResult{Index: i, Status: "missing_blob", Code: 409,
				Error: "upload the bytes first: PUT /tmx/archive/blobs/" + in.Blob}
		}
		if data, err = a.blobs.Get(ctx, in.Blob); err != nil {
			return itemError(i, 500, "read blob: "+err.Error())
		}
		if len(data) > maxFileAppendBytes {
			return itemError(i, 413, "append larger than 512 MiB")
		}
	}
	if len(data) == 0 {
		return itemError(i, 400, "no bytes to append")
	}
	chunk := HashOf(data)

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return itemError(i, 500, err.Error())
	}
	defer func() { _ = tx.Rollback() }()
	// One writer per file: appends to the same file serialise here.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 7))`, key+"\n"+in.Name); err != nil {
		return itemError(i, 500, "lock: "+err.Error())
	}
	v, err := a.resolveFile(ctx, tx, key, in.Name)
	if err != nil {
		return itemError(i, 500, "file state: "+err.Error())
	}
	st := v.state

	// Re-sends: the same bytes at the same place, or the whole file as it already is.
	if st != nil && st.Origin == fileOriginChunks {
		var dup bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.session_file_chunks
			WHERE session_id = $1 AND name = $2 AND generation = $3 AND "offset" = $4 AND blob = $5)`,
			key, in.Name, st.Generation, in.Offset, chunk).Scan(&dup); err != nil {
			return itemError(i, 500, err.Error())
		}
		if dup && !in.Reset {
			return ItemResult{Index: i, ID: key, Status: "exists", Code: 200, File: st}
		}
	}
	if st != nil && in.Offset == 0 && st.Length == int64(len(data)) && st.SHA256 == chunk {
		return ItemResult{Index: i, ID: key, Status: "exists", Code: 200, File: st}
	}

	conflict := func(msg string) ItemResult {
		cur := st
		if cur == nil {
			cur = &FileState{SessionID: key, Name: in.Name, SHA256: emptySHA256}
		}
		return ItemResult{Index: i, ID: key, Status: "conflict", Code: 409, Error: msg, File: cur}
	}
	var h hash.Hash
	gen, seq := v.tailGen+1, 0
	origin := chunkOriginAppend
	switch {
	case in.Reset:
		h, origin = sha256.New(), chunkOriginReset
	case st == nil:
		if in.Offset != 0 {
			return conflict(fmt.Sprintf("server has no bytes of this file; offset must be 0, got %d", in.Offset))
		}
		if in.BaseSHA256 != "" && in.BaseSHA256 != emptySHA256 {
			return conflict("base_sha256 doesn't match the server's (empty) file")
		}
		h = sha256.New()
	default:
		if in.Offset != st.Length {
			return conflict(fmt.Sprintf("offset %d doesn't match the server's length %d", in.Offset, st.Length))
		}
		if in.BaseSHA256 != "" && in.BaseSHA256 != st.SHA256 {
			return conflict("base_sha256 doesn't match the server's file hash")
		}
		if st.Origin == fileOriginSource {
			// First append after a full upload: that upload becomes chunk 0 of a new generation.
			if h, err = a.seedFromSource(ctx, tx, key, in.Name, gen, v.source, st.Length); err != nil {
				return itemError(i, 500, err.Error())
			}
			seq = 1
		} else {
			gen, seq = st.Generation, v.tailSeq+1
			h = sha256.New()
			if err = h.(encoding.BinaryUnmarshaler).UnmarshalBinary(v.hashState); err != nil {
				return itemError(i, 500, "hash state: "+err.Error())
			}
		}
	}
	h.Write(data)
	fileHash, hashState, err := sumAndState(h)
	if err != nil {
		return itemError(i, 500, err.Error())
	}
	if in.SHA256 != "" && in.SHA256 != fileHash {
		it := itemError(i, 422, "sha256 mismatch: the file after this append hashes to "+fileHash)
		it.ID = key
		return it
	}
	if in.DataBase64 != "" {
		if _, err = a.blobs.Put(ctx, data, "application/octet-stream", "import.source.chunk"); err != nil {
			return itemError(i, 500, "store chunk: "+err.Error())
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+schema+`.session_file_chunks (session_id, name, generation, seq,
		"offset", length, blob, file_sha256, hash_state, origin) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		key, in.Name, gen, seq, in.Offset, len(data), chunk, fileHash, hashState, origin); err != nil {
		return itemError(i, 500, "chunk index: "+err.Error())
	}
	// The session's analytics are refreshed when it is imported (the transpiled messages come
	// with the session upsert); journal it so the change is picked up either way.
	var hasSession bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.sessions WHERE id = $1)`, key).Scan(&hasSession); err != nil {
		return itemError(i, 500, err.Error())
	}
	if hasSession {
		if err = a.journal(ctx, tx, "session", key); err != nil {
			return itemError(i, 500, "analytics journal: "+err.Error())
		}
	}
	if err = tx.Commit(); err != nil {
		return itemError(i, 500, err.Error())
	}
	out := &FileState{SessionID: key, Name: in.Name, Length: in.Offset + int64(len(data)), SHA256: fileHash,
		Generation: gen, Chunks: seq + 1, Origin: fileOriginChunks, UpdatedAt: time.Now().UTC()}
	status, code := "appended", 200
	if st == nil || in.Reset {
		status, code = "created", 201
	}
	return ItemResult{Index: i, ID: key, Status: status, Code: code, File: out}
}

// seedFromSource writes chunk 0 of generation gen from a fully uploaded source blob and returns
// the hash after it. The blob is read once to get the hash state.
func (a *Archive) seedFromSource(ctx context.Context, tx querier, key, name string, gen int, blob string, size int64) (hash.Hash, error) {
	data, err := a.blobs.Get(ctx, blob)
	if err != nil {
		return nil, fmt.Errorf("read source %s: %w", blob, err)
	}
	if HashOf(data) != blob || int64(len(data)) != size {
		return nil, fmt.Errorf("source %s doesn't match its hash", blob)
	}
	h := sha256.New()
	h.Write(data)
	sum, state, err := sumAndState(h)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO `+schema+`.session_file_chunks (session_id, name, generation, seq,
		"offset", length, blob, file_sha256, hash_state, origin) VALUES ($1,$2,$3,0,0,$4,$5,$6,$7,$8)`,
		key, name, gen, size, blob, sum, state, chunkOriginSource); err != nil {
		return nil, fmt.Errorf("seed chunk: %w", err)
	}
	return h, nil
}

func sumAndState(h hash.Hash) (string, []byte, error) {
	state, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		return "", nil, fmt.Errorf("hash state: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), state, nil
}

// WriteFile writes the logical file (generation 0 = current) to w and returns its state.
// headers is called with the state before the first byte is written.
func (a *Archive) WriteFile(ctx context.Context, ref FileRef, generation int, headers func(*FileState), w io.Writer) error {
	key, ok := fileSessionKey(ref.Harness, ref.SessionID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrBadFileRef, errFileSessionNeeded)
	}
	if !validFileName(ref.Name) {
		return fmt.Errorf("%w: %s", ErrBadFileRef, errFileNameRequired)
	}
	v, err := a.resolveFile(ctx, a.db, key, ref.Name)
	if err != nil {
		return err
	}
	st := v.state
	var blobs []string
	switch {
	case st == nil && generation == 0:
		return ErrFileNotFound
	case generation == 0 && st.Origin == fileOriginSource:
		blobs = []string{v.source}
	default:
		if generation == 0 {
			generation = st.Generation
		}
		rows, errQ := a.db.QueryContext(ctx, `SELECT blob, "offset" + length, file_sha256, created_at FROM `+schema+
			`.session_file_chunks WHERE session_id = $1 AND name = $2 AND generation = $3 ORDER BY seq`, key, ref.Name, generation)
		if errQ != nil {
			return errQ
		}
		var last FileState
		for rows.Next() {
			var b string
			if err = rows.Scan(&b, &last.Length, &last.SHA256, &last.UpdatedAt); err != nil {
				_ = rows.Close()
				return err
			}
			blobs = append(blobs, b)
		}
		if err = closeRows(rows); err != nil {
			return err
		}
		if len(blobs) == 0 {
			return ErrFileNotFound
		}
		last.SessionID, last.Name, last.Generation, last.Chunks, last.Origin = key, ref.Name, generation, len(blobs), fileOriginChunks
		st = &last
	}
	if headers != nil {
		headers(st)
	}
	a.blobs.Prefetch(ctx, blobs)
	for _, b := range blobs {
		data, errGet := a.blobs.Get(ctx, b)
		if errGet != nil {
			return fmt.Errorf("chunk %s: %w", b, errGet)
		}
		if _, err = w.Write(data); err != nil {
			return err
		}
	}
	return nil
}
