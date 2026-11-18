package helps

import (
	"errors"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
)

// MetaRequestError preserves the final request's tools fact across error conversion.
// Tool attestation can fail for one request without condemning the credential.
func MetaRequestError(err error, request []byte) error {
	var status interface{ StatusCode() int }
	if err == nil || !errors.As(err, &status) || len(ojson.GetBytes(request, "tools").Array()) == 0 ||
		refusals.Detect("meta", status.StatusCode(), "", err.Error()) != refusals.MetaAttestation {
		return err
	}
	return metaAttestationError{error: err}
}

type metaAttestationError struct{ error }

func (e metaAttestationError) Unwrap() error     { return e.error }
func (e metaAttestationError) ErrorCode() string { return refusals.AttestationRefused }
