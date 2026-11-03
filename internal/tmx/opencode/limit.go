package opencode

import (
	"net/http"

	"github.com/tidwall/gjson"
)

// IsAccountLimit reports whether an upstream error is OpenCode Go's usage limit:
//
//	429 {"type":"error","error":{"type":"GoUsageLimitError","message":"Go usage limit exceeded"},
//	     "metadata":{"workspace":"wrk_…","limitName":"5 hour"}}
//
// The limit is the account's (one allowance pool for every model), so the credential cools down
// as a whole until Retry-After and the pool's next account serves every model.
func IsAccountLimit(status int, body []byte) bool {
	return status == http.StatusTooManyRequests && gjson.GetBytes(body, "error.type").String() == "GoUsageLimitError"
}
