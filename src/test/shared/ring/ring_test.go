package ring

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
)

func TestRoundTripAcrossWrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring")
	total := uint64(MinSlotSize*10 + 7)
	if err := Initialize(path, 2, MinSlotSize, total); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	producer, err := Open(ctx, path, true)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := Open(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	source := bytes.Repeat([]byte{0x5a}, int(total))
	done := make(chan error, 1)
	go func() {
		_, err := producer.Write(source)
		if closeErr := producer.Close(); err == nil {
			err = closeErr
		}
		done <- err
	}()
	got, err := io.ReadAll(consumer)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, source) {
		t.Fatal("ring changed bytes")
	}
}
