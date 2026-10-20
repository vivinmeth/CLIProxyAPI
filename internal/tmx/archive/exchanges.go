package archive

import (
	"context"
	"encoding/json"
	"time"
)

// ExchangeMeta is the index row of a captured exchange (blob columns are hashes).
type ExchangeMeta struct {
	TraceID            string          `json:"trace_id"`
	TS                 time.Time       `json:"ts"`
	FinishedAt         *time.Time      `json:"finished_at"`
	SessionID          string          `json:"session_id"`
	ClientSessionID    string          `json:"client_session_id"`
	Method             string          `json:"method"`
	URL                string          `json:"url"`
	Status             int             `json:"status"`
	Streaming          bool            `json:"streaming"`
	FirstChunkMs       int64           `json:"first_chunk_ms"`
	DurationMs         int64           `json:"duration_ms"`
	RequestHeaders     string          `json:"request_headers"`
	RequestBody        string          `json:"request_body"`
	ResponseHeaders    string          `json:"response_headers"`
	ResponseBody       string          `json:"response_body"`
	ResponseChunks     string          `json:"response_chunks"`
	UpstreamRequest    string          `json:"upstream_request"`
	UpstreamResponse   string          `json:"upstream_response"`
	WSTimeline         string          `json:"ws_timeline"`
	UpstreamWSTimeline string          `json:"upstream_ws_timeline"`
	Errors             json.RawMessage `json:"errors"`
	RequestBytes       int64           `json:"request_bytes"`
	ResponseBytes      int64           `json:"response_bytes"`
}

const exchangeColumns = `trace_id, ts, finished_at, session_id, method, url, status, streaming,
	first_chunk_ms, duration_ms, request_headers, request_body, response_headers, response_body,
	response_chunks, upstream_request, upstream_response, ws_timeline, upstream_ws_timeline, errors,
	request_bytes, response_bytes, client_session_id`

type scanner interface{ Scan(...any) error }

func scanExchange(r scanner) (*ExchangeMeta, error) {
	var e ExchangeMeta
	var errs []byte
	if err := r.Scan(&e.TraceID, &e.TS, &e.FinishedAt, &e.SessionID, &e.Method, &e.URL, &e.Status,
		&e.Streaming, &e.FirstChunkMs, &e.DurationMs, &e.RequestHeaders, &e.RequestBody,
		&e.ResponseHeaders, &e.ResponseBody, &e.ResponseChunks, &e.UpstreamRequest,
		&e.UpstreamResponse, &e.WSTimeline, &e.UpstreamWSTimeline, &errs, &e.RequestBytes,
		&e.ResponseBytes, &e.ClientSessionID); err != nil {
		return nil, err
	}
	e.Errors = errs
	return &e, nil
}

// ListExchanges lists captured exchanges newest first (index rows only).
func (a *Archive) ListExchanges(ctx context.Context, sessionID string, from, to time.Time, limit int) ([]ExchangeMeta, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := a.db.QueryContext(ctx, `SELECT `+exchangeColumns+` FROM `+schema+`.exchanges
		WHERE ts >= $1 AND ts < $2 AND ($3 = '' OR session_id = $3 OR client_session_id = $3) ORDER BY ts DESC LIMIT $4`,
		from, to, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []ExchangeMeta{}
	for rows.Next() {
		e, errScan := scanExchange(rows)
		if errScan != nil {
			return nil, errScan
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// GetExchange returns one exchange with every part rebuilt.
func (a *Archive) GetExchange(ctx context.Context, traceID string) (map[string]any, error) {
	meta, err := scanExchange(a.db.QueryRowContext(ctx, `SELECT `+exchangeColumns+` FROM `+schema+`.exchanges
		WHERE trace_id = $1`, traceID))
	if err != nil {
		return nil, err
	}
	out := map[string]any{"meta": meta}
	text := func(name, hash string) error {
		if hash == "" {
			return nil
		}
		data, errGet := a.blobs.Get(ctx, hash)
		if errGet != nil {
			return errGet
		}
		out[name] = string(data)
		return nil
	}
	jsonPart := func(name, hash string) error {
		if hash == "" {
			return nil
		}
		data, errGet := a.blobs.Get(ctx, hash)
		if errGet != nil {
			return errGet
		}
		out[name] = json.RawMessage(data)
		return nil
	}
	body := func(name, hash string) error {
		if hash == "" {
			return nil
		}
		data, errGet := a.rebuildBody(ctx, hash)
		if errGet != nil {
			return errGet
		}
		if json.Valid(data) {
			out[name] = json.RawMessage(data)
		} else {
			out[name] = string(data)
		}
		return nil
	}
	steps := []func() error{
		func() error { return jsonPart("request_headers", meta.RequestHeaders) },
		func() error { return body("request_body", meta.RequestBody) },
		func() error { return jsonPart("response_headers", meta.ResponseHeaders) },
		func() error { return text("response_body", meta.ResponseBody) },
		func() error { return jsonPart("response_chunks", meta.ResponseChunks) },
		func() error { return body("upstream_request", meta.UpstreamRequest) },
		func() error { return text("upstream_response", meta.UpstreamResponse) },
		func() error { return text("ws_timeline", meta.WSTimeline) },
		func() error { return text("upstream_ws_timeline", meta.UpstreamWSTimeline) },
	}
	for _, step := range steps {
		if err = step(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Blobs exposes the blob store to the HTTP layer.
func (a *Archive) Blobs() *BlobStore { return a.blobs }
