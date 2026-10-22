package spool

import (
	"os"
	"path/filepath"
	"sync"
)

// Open directories in this process; flockFile (unix) also keeps other processes out.
var (
	openMu   sync.Mutex
	openDirs = map[string]bool{}
)

type dirLock struct {
	dir string
	f   *os.File
}

func lockDir(dir string) (*dirLock, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	openMu.Lock()
	defer openMu.Unlock()
	if openDirs[abs] {
		return nil, ErrLocked
	}
	f, err := os.OpenFile(filepath.Join(abs, ".lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err = flockFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	openDirs[abs] = true
	return &dirLock{dir: abs, f: f}, nil
}

func (l *dirLock) Close() error {
	openMu.Lock()
	defer openMu.Unlock()
	delete(openDirs, l.dir)
	return l.f.Close() // closing the descriptor releases the flock
}
