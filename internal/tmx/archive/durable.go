package archive

// Durable capture: exchanges the archive can't store right now (the index database is down, a
// blob write failed, the capture queue is over its limits, or the archive is closing) are written
// to a durable on-disk spool (internal/tmx/spool) and replayed until they are stored. Nothing is
// dropped unless the spool is over its cap (TMX_SPOOL_ARCHIVE_MAX_MB, default 16 GiB), and that is
// counted in GET /tmx/durability.
//
// Spooled exchange: uint32 header length | header JSON | the captured bodies, back to back, in
// the order the header lists them. Credential headers are redacted before anything touches the
// disk; the request-log "rejected" row (which needs the client key) is computed before that and
// travels in the header.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/durability"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	tmxspool "github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/spool"
	log "github.com/sirupsen/logrus"
)

const (
	spooledExchangeVersion = 1
	archiveSpoolMaxBytes   = 16 << 30
	spooledHeaderMax       = 512 << 20
)

type spooledBody struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

type spooledExchange struct {
	V                    int                 `json:"v"`
	TraceID              string              `json:"trace_id"`
	SessionID            string              `json:"session_id"`
	ClientSessionID      string              `json:"client_session_id"`
	RequestModel         string              `json:"request_model"`
	RequestStreaming     bool                `json:"request_streaming"`
	URL                  string              `json:"url"`
	Method               string              `json:"method"`
	StartedAt            time.Time           `json:"started_at"`
	FinishedAt           time.Time           `json:"finished_at"`
	FirstChunkAt         time.Time           `json:"first_chunk_at"`
	Status               int                 `json:"status"`
	Streaming            bool                `json:"streaming"`
	RequestHeaders       map[string][]string `json:"request_headers"`
	ResponseHeaders      map[string][]string `json:"response_headers"`
	RequestBody          []byte              `json:"request_body,omitempty"`
	ResponseBody         []byte              `json:"response_body,omitempty"`
	Chunks               []capture.ChunkMark `json:"chunks,omitempty"`
	APIRequest           []byte              `json:"api_request,omitempty"`
	APIResponse          []byte              `json:"api_response,omitempty"`
	APIResponseAt        time.Time           `json:"api_response_at"`
	WebsocketTimeline    []byte              `json:"ws_timeline,omitempty"`
	APIWebsocketTimeline []byte              `json:"api_ws_timeline,omitempty"`
	Errors               []string            `json:"errors,omitempty"`
	Bodies               []spooledBody       `json:"bodies,omitempty"`
	// Rejected is the request-log row for an exchange refused before any account was tried
	// (rejectedRow), computed before the credential headers were redacted.
	Rejected *requestlog.Row `json:"rejected,omitempty"`
	// SpooledAt / Reason: when and why the exchange was spooled.
	SpooledAt time.Time `json:"spooled_at"`
	Reason    string    `json:"reason"`
}

// errBadSpooledExchange: the spooled bytes can't be decoded; retrying won't help.
var errBadSpooledExchange = errors.New("bad spooled exchange")

// openSpool opens the archive's durable spool: <blob spool>/durable/archive, or
// TMX_SPOOL_DIR/archive when that is set.
func (a *Archive) openSpool(blobSpool string) {
	dir := filepath.Join(blobSpool, "durable", "archive")
	if strings.TrimSpace(os.Getenv("TMX_SPOOL_DIR")) != "" {
		dir = tmxspool.Dir("archive")
	}
	sp, err := tmxspool.OpenFirstFree(dir, tmxspool.Options{Name: "archive", Deliver: a.deliverSpooled,
		MaxBatch: 1, MaxBytes: tmxspool.MaxBytesFromEnv("archive", archiveSpoolMaxBytes)})
	if err != nil {
		log.WithError(err).Error("tmx archive: NO DURABLE SPOOL, exchanges that can't be stored will be dropped")
		return
	}
	a.spool = sp
}

// spoolExchange writes ex to the durable spool (the caller still releases it). Without a spool
// the exchange is dropped, counted and logged.
func (a *Archive) spoolExchange(ex *capture.Exchange, reason string) {
	if a.spool == nil {
		if a.dropped.Add(1)%20 == 1 {
			log.Errorf("tmx archive: EXCHANGE DROPPED, no spool to keep it (%d dropped since start)", a.dropped.Load())
		}
		return
	}
	hint := exchangeSpoolSize(ex)
	// A failed append is counted (and logged) by the spool.
	_ = a.spool.AppendFunc(hint, func(w io.Writer) error { return encodeExchange(w, ex, reason) })
}

func exchangeSpoolSize(ex *capture.Exchange) int64 {
	size := int64(4096 + len(ex.RequestBody) + len(ex.ResponseBody) + len(ex.APIRequest) + len(ex.APIResponse) +
		len(ex.WebsocketTimeline) + len(ex.APIWebsocketTimeline))
	for _, b := range ex.Bodies {
		size += b.Size()
	}
	return size
}

func encodeExchange(w io.Writer, ex *capture.Exchange, reason string) error {
	h := spooledExchange{V: spooledExchangeVersion, TraceID: ex.TraceID, SessionID: ex.SessionID,
		ClientSessionID: ex.ClientSessionID, RequestModel: ex.RequestModel, RequestStreaming: ex.RequestStreaming,
		URL: ex.URL, Method: ex.Method, StartedAt: ex.StartedAt, FinishedAt: ex.FinishedAt,
		FirstChunkAt: ex.FirstChunkAt, Status: ex.Status, Streaming: ex.Streaming,
		RequestHeaders: redactHeaders(ex.RequestHeaders), ResponseHeaders: redactHeaders(ex.ResponseHeaders),
		RequestBody: ex.RequestBody, ResponseBody: ex.ResponseBody, Chunks: ex.Chunks, APIRequest: ex.APIRequest,
		APIResponse: ex.APIResponse, APIResponseAt: ex.APIResponseAt, WebsocketTimeline: ex.WebsocketTimeline,
		APIWebsocketTimeline: ex.APIWebsocketTimeline, Errors: append([]string(nil), ex.Errors...),
		SpooledAt: time.Now().UTC(), Reason: reason}
	if rej, ok := rejectedRow(ex); ok {
		h.Rejected = &rej
	}
	names := make([]string, 0, len(ex.Bodies))
	for name := range ex.Bodies {
		names = append(names, name)
	}
	sort.Strings(names)
	var readers []io.Reader
	for _, name := range names {
		b := ex.Bodies[name]
		if b == nil {
			continue
		}
		r, err := b.Reader()
		if err != nil {
			// The capture of this body already failed (it is gone); keep the rest of the exchange.
			h.Errors = append(h.Errors, fmt.Sprintf("tmx capture: %s body lost before spooling: %v", name, err))
			continue
		}
		h.Bodies = append(h.Bodies, spooledBody{Name: name, Size: b.Size()})
		readers = append(readers, r)
	}
	head, err := json.Marshal(&h)
	if err != nil {
		return err
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(head)))
	if _, err = w.Write(n[:]); err != nil {
		return err
	}
	if _, err = w.Write(head); err != nil {
		return err
	}
	for i, r := range readers {
		copied, errC := io.Copy(w, r)
		if errC != nil {
			return errC
		}
		if copied != h.Bodies[i].Size {
			return fmt.Errorf("body %s: copied %d of %d bytes", h.Bodies[i].Name, copied, h.Bodies[i].Size)
		}
	}
	return nil
}

// decodeExchange rebuilds a spooled exchange; its bodies go through the bounded capture writer
// (memory up to the live budget, else a spill file). The caller releases it.
func decodeExchange(r io.Reader) (*capture.Exchange, *requestlog.Row, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, nil, fmt.Errorf("%w: header length: %v", errBadSpooledExchange, err)
	}
	size := binary.BigEndian.Uint32(n[:])
	if size > spooledHeaderMax {
		return nil, nil, fmt.Errorf("%w: header of %d bytes", errBadSpooledExchange, size)
	}
	head := make([]byte, size)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, nil, fmt.Errorf("%w: header: %v", errBadSpooledExchange, err)
	}
	var h spooledExchange
	if err := json.Unmarshal(head, &h); err != nil || h.V != spooledExchangeVersion {
		return nil, nil, fmt.Errorf("%w: header json (v%d): %v", errBadSpooledExchange, h.V, err)
	}
	ex := &capture.Exchange{TraceID: h.TraceID, SessionID: h.SessionID, ClientSessionID: h.ClientSessionID,
		RequestModel: h.RequestModel, RequestStreaming: h.RequestStreaming, URL: h.URL, Method: h.Method,
		StartedAt: h.StartedAt, FinishedAt: h.FinishedAt, FirstChunkAt: h.FirstChunkAt, Status: h.Status,
		Streaming: h.Streaming, RequestHeaders: h.RequestHeaders, ResponseHeaders: h.ResponseHeaders,
		RequestBody: h.RequestBody, ResponseBody: h.ResponseBody, Chunks: h.Chunks, APIRequest: h.APIRequest,
		APIResponse: h.APIResponse, APIResponseAt: h.APIResponseAt, WebsocketTimeline: h.WebsocketTimeline,
		APIWebsocketTimeline: h.APIWebsocketTimeline, Errors: h.Errors}
	if len(h.Bodies) > 0 {
		ex.Bodies = make(map[string]*capture.Body, len(h.Bodies))
		group := &capture.Group{}
		for _, sb := range h.Bodies {
			b := capture.CaptureInGroup(nil, readerSource{io.LimitReader(r, sb.Size)}, group)
			ex.Bodies[sb.Name] = b
			if err := b.Err(); err != nil {
				ex.Release()
				return nil, nil, fmt.Errorf("body %s: %w", sb.Name, err) // disk trouble: retried
			}
			if b.Size() != sb.Size {
				ex.Release()
				return nil, nil, fmt.Errorf("%w: body %s has %d of %d bytes", errBadSpooledExchange, sb.Name, b.Size(), sb.Size)
			}
		}
	}
	return ex, h.Rejected, nil
}

type readerSource struct{ r io.Reader }

func (s readerSource) WriteTo(w io.Writer) (int64, error) { return io.Copy(w, s.r) }

// deliverSpooled is the spool's replay sink: decode and store one exchange at a time.
func (a *Archive) deliverSpooled(ctx context.Context, items []tmxspool.Item) (int, error) {
	for i, it := range items {
		ex, rej, err := decodeExchange(it.Reader())
		if err != nil {
			if errors.Is(err, errBadSpooledExchange) {
				return i, tmxspool.Permanent(err)
			}
			return i, err
		}
		err = a.storeExchangeWith(ctx, ex, func(*capture.Exchange) (requestlog.Row, bool) {
			if rej == nil {
				return requestlog.Row{}, false
			}
			return *rej, true
		})
		ex.Release()
		if err != nil {
			if isDataError(err) {
				return i, tmxspool.Permanent(err)
			}
			return i, err
		}
		a.captured.Add(1)
	}
	return len(items), nil
}

// isDataError: the database refused the row's content; storing it again would fail again.
func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	return pgErr.Code[:2] == "22" || pgErr.Code[:2] == "23"
}

// Durability is the archive capture writer's entry in GET /tmx/durability.
func (a *Archive) Durability() durability.Stats {
	var d durability.Stats
	if a.spool != nil {
		d = a.spool.Stats()
	}
	d.Queued = int64(len(a.queue))
	d.Dropped += a.dropped.Load()
	return d
}

// captureDurability: bodies lost because the live capture couldn't spill to disk (capture.Body).
func captureDurability() durability.Stats {
	return durability.Stats{Dropped: capture.Dropped()}
}
