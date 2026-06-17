package eventlog

import (
	"io"
	"os"

	v1 "github.com/supunhg/kairos/api/v1"
)

type iterator struct {
	file        *os.File
	dec         *Decoder
	opts        IterOptions
	current     *v1.Event
	err         error
	count       int
	started     bool
	startOffset int64
	closed      bool
}

func newIterator(file *os.File, index map[string]int64, opts IterOptions) Iterator {
	it := &iterator{
		file: file,
		opts: opts,
	}
	if opts.Limit <= 0 {
		it.opts.Limit = 1000
	}
	it.startOffset = it.findStartOffset(index)
	return it
}

func (it *iterator) findStartOffset(index map[string]int64) int64 {
	if it.opts.AfterID != "" {
		if off, ok := index[it.opts.AfterID]; ok {
			return off
		}
	}
	if it.opts.Reverse {
		var max int64
		for _, off := range index {
			if off > max {
				max = off
			}
		}
		return max
	}
	return 0
}

func (it *iterator) Next() bool {
	if it.closed || it.err != nil || it.count >= it.opts.Limit {
		return false
	}

	if !it.started {
		it.started = true
		if _, err := it.file.Seek(it.startOffset, io.SeekStart); err != nil {
			it.err = err
			return false
		}
		it.dec = NewDecoder(it.file)

		// if afterID, skip that event (just resume after it)
		if it.opts.AfterID != "" {
			ev, err := it.dec.Decode()
			if err != nil {
				if err == io.EOF {
					return false
				}
				it.err = err
				return false
			}
			// Verify we skipped the right one
			if ev.Id != it.opts.AfterID {
				if it.matches(ev) {
					it.count++
					it.current = ev
					return true
				}
			}
		}
	}

	for it.count < it.opts.Limit {
		ev, err := it.dec.Decode()
		if err == io.EOF {
			return false
		}
		if err != nil {
			it.err = err
			return false
		}

		if !it.matches(ev) {
			continue
		}

		it.count++
		it.current = ev
		return true
	}

	return false
}

func (it *iterator) matches(ev *v1.Event) bool {
	if it.opts.GroupID != "" && ev.GroupId != it.opts.GroupID {
		return false
	}
	if it.opts.SessionID != "" && ev.SessionId != it.opts.SessionID {
		return false
	}
	if it.opts.SinceHLC > 0 && ev.HlcTimestamp < it.opts.SinceHLC {
		return false
	}
	if it.opts.UntilHLC > 0 && ev.HlcTimestamp > it.opts.UntilHLC {
		return false
	}
	if it.opts.BeforeID != "" && ev.Id == it.opts.BeforeID {
		return false
	}
	return true
}

func (it *iterator) Event() *v1.Event {
	return it.current
}

func (it *iterator) Err() error {
	return it.err
}

func (it *iterator) Close() error {
	it.closed = true
	return nil
}
