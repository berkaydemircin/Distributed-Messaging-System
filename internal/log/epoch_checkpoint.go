package log

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

var ErrMalformedEpochCheckpoint = errors.New("malformed leader epoch checkpoint")

const epochCheckpointVersion = 0

type writableFile interface {
	Write(p []byte) (n int, err error)
	Sync() error
	Close() error
}

type checkpointFileOps struct {
	openWrite func(path string) (writableFile, error)
	rename    func(oldpath, newpath string) error
	syncDir   func(dir string) error
}

func realCheckpointFileOps() checkpointFileOps {
	return checkpointFileOps{
		openWrite: func(path string) (writableFile, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		},
		rename: os.Rename,
		syncDir: func(dir string) error {
			// fsync fallback for windows obviously less durable currently, maybe theres an alternative? probably not needed for now
			if runtime.GOOS == "windows" {
				return nil
			}

			d, err := os.Open(dir)
			if err != nil {
				return err
			}
			defer d.Close()
			return d.Sync()
		},
	}
}

type EpochCheckpoint struct {
	path string
	mu   sync.Mutex
	ops  checkpointFileOps
}

func NewEpochCheckpoint(path string) *EpochCheckpoint {
	return &EpochCheckpoint{path: path, ops: realCheckpointFileOps()}
}

func (c *EpochCheckpoint) Read() ([]EpochEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	f, err := os.Open(c.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("epochcheckpoint: open %s: %w", c.path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)

	if !scanner.Scan() {
		if serr := scanner.Err(); serr != nil {
			return nil, fmt.Errorf("epochcheckpoint: read %s: %w", c.path, serr)
		}
		return nil, nil // empty file
	}
	version, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || version != epochCheckpointVersion {
		return nil, fmt.Errorf("%w: %s: unsupported version %q", ErrMalformedEpochCheckpoint, c.path, scanner.Text())
	}

	if !scanner.Scan() {
		if serr := scanner.Err(); serr != nil {
			return nil, fmt.Errorf("epochcheckpoint: read %s: %w", c.path, serr)
		}
		return nil, fmt.Errorf("%w: %s: missing count line", ErrMalformedEpochCheckpoint, c.path)
	}
	count, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil || count < 0 {
		return nil, fmt.Errorf("%w: %s: invalid entry count %q", ErrMalformedEpochCheckpoint, c.path, scanner.Text())
	}

	entries := make([]EpochEntry, 0)
	for i := 0; i < count; i++ {
		if !scanner.Scan() {
			if serr := scanner.Err(); serr != nil {
				return nil, fmt.Errorf("epochcheckpoint: read %s: %w", c.path, serr)
			}
			return nil, fmt.Errorf("%w: %s: expected %d entries, found %d", ErrMalformedEpochCheckpoint, c.path, count, i)
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			return nil, fmt.Errorf("%w: %s: entry %d has %d fields, want 2", ErrMalformedEpochCheckpoint, c.path, i, len(fields))
		}

		epoch, err := strconv.ParseInt(fields[0], 10, 32)
		if err != nil || epoch < 0 {
			return nil, fmt.Errorf("%w: %s: entry %d: invalid epoch %q", ErrMalformedEpochCheckpoint, c.path, i, fields[0])
		}
		offset, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || offset < 0 {
			return nil, fmt.Errorf("%w: %s: entry %d: invalid offset %q", ErrMalformedEpochCheckpoint, c.path, i, fields[1])
		}

		entries = append(entries, EpochEntry{Epoch: int32(epoch), StartOffset: uint64(offset)})
	}

	if scanner.Scan() {
		return nil, fmt.Errorf("%w: %s: unexpected trailing content after %d entries",
			ErrMalformedEpochCheckpoint, c.path, count)
	}
	if serr := scanner.Err(); serr != nil {
		return nil, fmt.Errorf("epochcheckpoint: read %s: %w", c.path, serr)
	}

	return entries, nil
}

func (c *EpochCheckpoint) Write(entries []EpochEntry) error {
	for i, e := range entries {
		if e.Epoch < 0 {
			return fmt.Errorf("epochcheckpoint: entry %d: negative epoch %d", i, e.Epoch)
		}
		if e.StartOffset > math.MaxInt64 {
			return fmt.Errorf("epochcheckpoint: entry %d: start offset %d exceeds int64 range and could never be read back",
				i, e.StartOffset)
		}
		if i > 0 {
			prev := entries[i-1]
			if e.Epoch <= prev.Epoch {
				return fmt.Errorf("epochcheckpoint: entry %d epoch %d does not strictly increase from %d",
					i, e.Epoch, prev.Epoch)
			}
			if e.StartOffset <= prev.StartOffset {
				return fmt.Errorf("epochcheckpoint: entry %d start offset %d does not strictly increase from %d",
					i, e.StartOffset, prev.StartOffset)
			}
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "%d\n", epochCheckpointVersion)
	fmt.Fprintf(&sb, "%d\n", len(entries))
	for _, e := range entries {
		fmt.Fprintf(&sb, "%d %d\n", e.Epoch, e.StartOffset)
	}
	data := []byte(sb.String())

	c.mu.Lock()
	defer c.mu.Unlock()

	tmpPath := c.path + ".tmp"

	f, err := c.ops.openWrite(tmpPath)
	if err != nil {
		return fmt.Errorf("epochcheckpoint: open temp file %s: %w", tmpPath, err)
	}

	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}

	// im just making the cleanup explicit, i know i can just not have _ = :)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("epochcheckpoint: write temp file %s: %w", tmpPath, err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("epochcheckpoint: sync temp file %s: %w", tmpPath, err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("epochcheckpoint: close temp file %s: %w", tmpPath, err)
	}

	if err := c.ops.rename(tmpPath, c.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("epochcheckpoint: rename %s to %s: %w", tmpPath, c.path, err)
	}

	if err := c.ops.syncDir(filepath.Dir(c.path)); err != nil {
		return fmt.Errorf("epochcheckpoint: sync parent directory of %s: %w", c.path, err)
	}

	return nil
}
