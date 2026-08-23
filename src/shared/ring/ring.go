package ring

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const (
	HeaderSize      = 4096
	DefaultSlots    = 8
	MaxSlotSize     = 16 << 20
	MinSlotSize     = 1 << 20
	MaxStreamLength = 1 << 40
	versionOffset   = 136
	slotsOffset     = 140
	slotSizeOffset  = 144
	totalOffset     = 152
	headOffset      = 0
	tailOffset      = 64
)

var magic = [8]byte{'A', 'I', 'O', 'R', 'I', 'N', 'G', '1'}

type Ring struct {
	file       *os.File
	data       []byte
	head       *uint64
	tail       *uint64
	slots      uint64
	slotSize   uint64
	total      uint64
	position   uint64
	frameBytes uint64
	framePos   uint64
	ctx        context.Context
	producer   bool
}

func Initialize(path string, slots int, slotSize, total uint64) error {
	if slots < 2 || slots > 64 || slotSize < MinSlotSize || slotSize > MaxSlotSize || total == 0 || total > MaxStreamLength {
		return errors.New("invalid ring bounds")
	}
	size := uint64(HeaderSize) + uint64(slots)*slotSize
	if size > uint64(^uint(0)>>1) {
		return errors.New("ring mapping is too large")
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0660)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(int64(size)); err != nil {
		return err
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	defer syscall.Munmap(data)
	copy(data[128:136], magic[:])
	binary.LittleEndian.PutUint32(data[versionOffset:versionOffset+4], 1)
	binary.LittleEndian.PutUint32(data[slotsOffset:slotsOffset+4], uint32(slots))
	binary.LittleEndian.PutUint64(data[slotSizeOffset:slotSizeOffset+8], slotSize)
	binary.LittleEndian.PutUint64(data[totalOffset:totalOffset+8], total)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&data[headOffset])), 0)
	atomic.StoreUint64((*uint64)(unsafe.Pointer(&data[tailOffset])), 0)
	return nil
}

func Open(ctx context.Context, path string, producer bool) (*Ring, error) {
	file, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Mode()&os.ModeType != 0 || info.Size() < HeaderSize {
		file.Close()
		return nil, errors.New("ring is not a regular mapped file")
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, int(info.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}
	if string(data[128:136]) != string(magic[:]) || binary.LittleEndian.Uint32(data[versionOffset:versionOffset+4]) != 1 {
		syscall.Munmap(data)
		file.Close()
		return nil, errors.New("invalid ring magic or version")
	}
	slots := uint64(binary.LittleEndian.Uint32(data[slotsOffset : slotsOffset+4]))
	slotSize := binary.LittleEndian.Uint64(data[slotSizeOffset : slotSizeOffset+8])
	total := binary.LittleEndian.Uint64(data[totalOffset : totalOffset+8])
	want := uint64(HeaderSize) + slots*slotSize
	if slots < 2 || slots > 64 || slotSize < MinSlotSize || slotSize > MaxSlotSize || total == 0 || total > MaxStreamLength || want != uint64(len(data)) {
		syscall.Munmap(data)
		file.Close()
		return nil, errors.New("invalid ring bounds or mapping length")
	}
	return &Ring{file: file, data: data, head: (*uint64)(unsafe.Pointer(&data[headOffset])), tail: (*uint64)(unsafe.Pointer(&data[tailOffset])), slots: slots, slotSize: slotSize, total: total, ctx: ctx, producer: producer}, nil
}

func (r *Ring) Total() uint64 { return r.total }

func (r *Ring) Read(buffer []byte) (int, error) {
	if r.producer {
		return 0, errors.New("producer ring cannot be read")
	}
	if r.position == r.total {
		return 0, io.EOF
	}
	for r.framePos == r.frameBytes {
		head, tail := atomic.LoadUint64(r.head), atomic.LoadUint64(r.tail)
		if head < tail || head-tail > r.slots {
			return 0, errors.New("corrupt ring counters")
		}
		if head == tail {
			if err := wait(r.ctx); err != nil {
				return 0, err
			}
			continue
		}
		r.frameBytes = min(r.slotSize, r.total-r.position)
		r.framePos = 0
		break
	}
	amount := min(uint64(len(buffer)), r.frameBytes-r.framePos)
	tail := atomic.LoadUint64(r.tail)
	start := uint64(HeaderSize) + (tail%r.slots)*r.slotSize + r.framePos
	copy(buffer, r.data[start:start+amount])
	r.framePos += amount
	r.position += amount
	if r.framePos == r.frameBytes {
		atomic.StoreUint64(r.tail, tail+1)
	}
	return int(amount), nil
}

func (r *Ring) Write(buffer []byte) (int, error) {
	if !r.producer {
		return 0, errors.New("consumer ring cannot be written")
	}
	if r.position+uint64(len(buffer)) > r.total {
		return 0, errors.New("write exceeds declared ring length")
	}
	written := 0
	for written < len(buffer) {
		for r.framePos == r.frameBytes {
			head, tail := atomic.LoadUint64(r.head), atomic.LoadUint64(r.tail)
			if head < tail || head-tail > r.slots {
				return written, errors.New("corrupt ring counters")
			}
			if head-tail == r.slots {
				if err := wait(r.ctx); err != nil {
					return written, err
				}
				continue
			}
			r.frameBytes = min(r.slotSize, r.total-r.position)
			r.framePos = 0
			break
		}
		amount := min(uint64(len(buffer)-written), r.frameBytes-r.framePos)
		head := atomic.LoadUint64(r.head)
		start := uint64(HeaderSize) + (head%r.slots)*r.slotSize + r.framePos
		copy(r.data[start:start+amount], buffer[written:written+int(amount)])
		r.framePos += amount
		r.position += amount
		written += int(amount)
		if r.framePos == r.frameBytes {
			atomic.StoreUint64(r.head, head+1)
		}
	}
	return written, nil
}

func (r *Ring) Close() error {
	var result error
	if r.producer && r.position != r.total {
		result = fmt.Errorf("ring closed after %d of %d bytes", r.position, r.total)
	}
	if err := syscall.Munmap(r.data); result == nil && err != nil {
		result = err
	}
	if err := r.file.Close(); result == nil && err != nil {
		result = err
	}
	return result
}

func wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		runtime.Gosched()
		return nil
	}
}
