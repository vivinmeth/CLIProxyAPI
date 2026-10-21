package auth

// tokenmaxx fork: the admission mutex serializes every pick under reset-soonest so in-flight
// counts read while ranking match the reservation. Session affinity's body parsing (explicit
// session identity, LCP turn fingerprints) doesn't need that mutex, so it runs before the mutex
// is taken and the selector reads the memoised results inside it.

import (
	"context"
	"net/http"
	"reflect"
	"sync/atomic"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	cliproxysession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

type tmxSessionPrepKey struct{}
type tmxUnderAdmissionKey struct{}

// tmxSessionParsesUnderAdmission counts request bodies parsed while the admission mutex was
// held, i.e. memo misses that serialized every other pick behind them.
var tmxSessionParsesUnderAdmission atomic.Int64

func countSessionParse(ctx context.Context) {
	if ctx != nil && ctx.Value(tmxUnderAdmissionKey{}) != nil {
		tmxSessionParsesUnderAdmission.Add(1)
	}
}

// tmxSessionPrep memoises one pick's body-derived session inputs. Each result is reused only for
// the same payload, headers and (for the session identity) the metadata ExtractSessionInfo reads.
type tmxSessionPrep struct {
	payload []byte
	headers http.Header

	hasInfo bool
	infoFor tmxSessionInfoMeta
	info    cliproxysession.SessionInfo
	infoOK  bool

	hasLCP           bool
	matcher          *cliproxysession.MerklePrefixMatcher
	format           sdktranslator.Format
	fingerprints     []string
	minPrefixLength  int
	tailFingerprints []string
	envDigest        string
}

// tmxSessionInfoMeta is the metadata ExtractSessionInfo depends on, read the way it reads it.
type tmxSessionInfoMeta struct {
	scope, execution, lcp, parent string
	compaction                    bool
}

func sessionInfoMetaOf(metadata map[string]any) tmxSessionInfoMeta {
	var meta tmxSessionInfoMeta
	meta.scope, _ = metadata[cliproxyexecutor.CallerScopeMetadataKey].(string)
	meta.execution, _ = metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string)
	meta.lcp, _ = metadata[cliproxyexecutor.LCPAffinitySessionIDMetadataKey].(string)
	meta.parent, _ = metadata[cliproxyexecutor.ParentSessionIDMetadataKey].(string)
	meta.compaction, _ = metadata[cliproxyexecutor.IsCompactionMetadataKey].(bool)
	return meta
}

func sameBytes(a, b []byte) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}

func sameHeaders(a, b http.Header) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// prepareTMXSession computes the session inputs of a pick for selector, outside the admission
// mutex. Selectors without session affinity need none.
func prepareTMXSession(ctx context.Context, selector Selector, opts cliproxyexecutor.Options) context.Context {
	affinity, ok := selector.(*SessionAffinitySelector)
	if !ok || affinity == nil {
		return ctx
	}
	prep := &tmxSessionPrep{payload: opts.OriginalRequest, headers: opts.Headers}
	prep.hasInfo = true
	prep.infoFor = sessionInfoMetaOf(opts.Metadata)
	prep.info, prep.infoOK = cliproxysession.ExtractSessionInfo(opts.Headers, opts.OriginalRequest, opts.Metadata)
	explicit := prep.infoOK && prep.info.ClientType != "lcp"
	if !explicit && affinity.matcher != nil {
		prep.hasLCP = true
		prep.matcher = affinity.matcher
		prep.format = opts.SourceFormat
		turns := cliproxysession.ExtractCanonicalTurns(opts.SourceFormat, opts.OriginalRequest)
		if len(turns) > 0 {
			prep.fingerprints, prep.minPrefixLength, prep.tailFingerprints, prep.envDigest = affinity.matcher.PrepareExt(turns)
		}
	}
	return context.WithValue(ctx, tmxSessionPrepKey{}, prep)
}

func tmxSessionPrepFrom(ctx context.Context, headers http.Header, payload []byte) *tmxSessionPrep {
	if ctx == nil {
		return nil
	}
	prep, _ := ctx.Value(tmxSessionPrepKey{}).(*tmxSessionPrep)
	if prep == nil || !sameBytes(prep.payload, payload) || !sameHeaders(prep.headers, headers) {
		return nil
	}
	return prep
}

// tmxExtractSessionInfo is cliproxysession.ExtractSessionInfo, memoised for the pick.
func tmxExtractSessionInfo(ctx context.Context, headers http.Header, payload []byte, metadata map[string]any) (cliproxysession.SessionInfo, bool) {
	if prep := tmxSessionPrepFrom(ctx, headers, payload); prep != nil && prep.hasInfo && prep.infoFor == sessionInfoMetaOf(metadata) {
		return prep.info, prep.infoOK
	}
	countSessionParse(ctx)
	return cliproxysession.ExtractSessionInfo(headers, payload, metadata)
}

// tmxPrepareLCP returns the LCP fingerprints of the request, memoised for the pick.
func tmxPrepareLCP(ctx context.Context, matcher *cliproxysession.MerklePrefixMatcher, format sdktranslator.Format, payload []byte, headers http.Header) ([]string, int, []string, string) {
	if prep := tmxSessionPrepFrom(ctx, headers, payload); prep != nil && prep.hasLCP && prep.matcher == matcher && prep.format == format {
		return prep.fingerprints, prep.minPrefixLength, prep.tailFingerprints, prep.envDigest
	}
	countSessionParse(ctx)
	turns := cliproxysession.ExtractCanonicalTurns(format, payload)
	if len(turns) == 0 {
		return nil, 0, nil, ""
	}
	return matcher.PrepareExt(turns)
}
