//go:build !unix

package spool

import "os"

// Other platforms only get the in-process guard.
func flockFile(*os.File) error { return nil }
