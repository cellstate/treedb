package simplefs

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

// before: [0 -- --][2 -- --][4 -- --][6 -- --][8 -- --][10 EOF]
// inject:                [3 -- -- -- -- ]
//  after: [0 -- --][2 --][3 -- -- -- -- ][7--][8 -- --][10 EOF]
func TestInjectChunkMiddle(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, []byte{0x06, 0x07}, false},
		&chunk{8, []byte{0x08, 0x09}, false},
		&chunk{10, nil, true}, //EOF
	}}

	err := cbuf.inject(3, []byte{0x03, 0x04, 0x05, 0x06})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 6 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[3].o != 7 {
		t.Fatalf("expected offset to be shifted, got: %+v", cbuf.chunks[3])
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 -- --][2 -- --][4 -- --][6 EOF]
// inject:                         [5 -- -- -- -- ]
//  after: [0 -- --][2 -- --][4 --][5 -- -- -- -- ][9 EOF]
func TestInjectChunkEnd(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, nil, true}, //EOF
	}}

	err := cbuf.inject(5, []byte{0x05, 0x06, 0x07, 0x08})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 5 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[len(cbuf.chunks)-1].o != 9 {
		t.Fatal("expected end offset to be this value")
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 -- --][2 -- --][4 -- --][6 EOF]
// inject:               [3 -- -- -- -- ]
//  after: [0 -- --][2 --[3 -- -- -- -- ][7 EOF]
func TestInjectChunkMiddleEnd(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, nil, true}, //EOF
	}}

	err := cbuf.inject(3, []byte{0x03, 0x04, 0x05, 0x06})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 4 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[len(cbuf.chunks)-1].o != 7 {
		t.Fatal("expected end offset to be this value")
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 -- --][2 -- --][4 -- --][6 EOF]
// inject: [0 -- --   -- -- -- ]
// after:  [0 -- --   -- -- -- ][5 --][6 EOF]
func TestInjectChunkMiddleStart(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, nil, true}, //EOF
	}}

	err := cbuf.inject(0, []byte{0x00, 0x01, 0x02, 0x03, 0x04})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 3 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[1].o != 5 {
		t.Fatal("expected end offset to be this value")
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 -- --][2 -- --][4 -- --][6 EOF]
// inject: [0 -- --   -- -- ]
// after:  [0 -- --   -- -- ][4 -- --][6 EOF]
func TestInjectChunkPreciseTwoBlockOverwrite(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, nil, true}, //EOF
	}}

	err := cbuf.inject(0, []byte{0x00, 0x01, 0x02, 0x03})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 3 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[1].o != 4 {
		t.Fatal("expected end offset to be this value")
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 -- --][2 -- --][4 -- --][6 EOF]
// inject:          [2 -- ++]
// after:  [0 -- -- [2 -- --][4 -- --][6 EOF]
func TestInjectChunkPreciseOneBlockOverwrite(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, []byte{0x00, 0x01}, false},
		&chunk{2, []byte{0x02, 0x03}, false},
		&chunk{4, []byte{0x04, 0x05}, false},
		&chunk{6, nil, true}, //EOF
	}}

	err := cbuf.inject(2, []byte{0x88, 0x88})
	if err != nil {
		t.Error(err)
	}

	if len(cbuf.chunks) != 4 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[1].o != 2 {
		t.Fatal("expected end offset to be this value")
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x88, 0x88, 0x04, 0x05}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

// before: [0 EOF]
// inject: [0 -- -- ]
// after:  [0 -- -- ][2 EOF]
func TestInjectChunkEmpty(t *testing.T) {
	cbuf := &ChunkBuf{chunks: []*chunk{
		&chunk{0, nil, true}, //EOF
	}}

	err := cbuf.inject(0, []byte{0x00, 0x01})
	if err != nil {
		t.Error(err)
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
	}

	if len(cbuf.chunks) != 2 {
		t.Fatalf("expected this many chunks, got: %+v", cbuf.chunks)
	}

	if !cbuf.chunks[len(cbuf.chunks)-1].eof {
		t.Fatal("expected chunk EOF at the end")
	}

	if cbuf.chunks[1].o != 2 {
		t.Fatal("expected end offset to be this value")
	}

	if !bytes.Equal(result, []byte{0x00, 0x01}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}
}

func TestWriteFlushPasMaxSize(t *testing.T) {
	cbuf, err := NewChunkBuf()
	if err != nil {
		t.Fatalf("didn't expect error, got: %v", err)
	}

	input := make([]byte, 2*miB)
	rand.Read(input)

	n, err := cbuf.Write(input) //write to chunker
	if err != nil {
		t.Fatalf("didn't expect error, got: %v", err)
	}

	if len(cbuf.chunks) < 2 {
		t.Fatal("expected at least two chunks at this point")
	}

	err = cbuf.flush() //flush remaining from chunker
	if err != nil {
		t.Fatalf("didn't expect error, got: %v", err)
	}

	if n != len(input) {
		t.Fatalf("expected this many bytes to have been written, got: %v", n)
	}

	output := []byte{}
	totalN := 0
	for _, c := range cbuf.chunks {
		totalN = totalN + len(c.d)
		output = append(output, c.d...)
	}

	if len(input) != totalN {
		t.Fatal("expected nr of output bytes to equal input bytes")
	}

	if !bytes.Equal(input, output) {
		t.Fatalf("expected output to be equal to input")
	}
}

func TestWriteAfterFlush(t *testing.T) {
	fmt.Println("write after flush!")
	cbuf, err := NewChunkBuf()
	if err != nil {
		t.Fatalf("didn't expect error, got: %v", err)
	}

	input1 := make([]byte, 256*kiB)
	rand.Read(input1)
	input2 := make([]byte, 128*kiB)
	rand.Read(input2)
	input3 := make([]byte, 128*kiB)
	rand.Read(input3)

	n, err := cbuf.Write(input1)
	if err != nil || n != len(input1) {
		t.Fatalf("failed to write: %v", err)
	}

	n, err = cbuf.Write(input2)
	if err != nil || n != len(input2) {
		t.Fatalf("failed to write: %v", err)
	}

	err = cbuf.flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	n, err = cbuf.Write(input3)
	if err != nil || n != len(input3) {
		t.Fatalf("failed to write: %v", err)
	}

	err = cbuf.flush()
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	output := []byte{}
	totalN := 0
	for _, c := range cbuf.chunks {
		fmt.Println(c.o, c.eof)
		totalN = totalN + len(c.d)
		output = append(output, c.d...)
	}

	if 512*kiB != uint64(totalN) {
		t.Fatalf("expected nr of output bytes to equal input bytes, total: %d Kib", (totalN / kiB))
	}

	if !bytes.Equal(input1, output[:(256*kiB)]) {
		t.Fatalf("expected output to be equal to input")
	}

	if !bytes.Equal(input2, output[(256*kiB):(384*kiB)]) {
		t.Fatalf("expected output to be equal to input")
	}

	if !bytes.Equal(input3, output[(384*kiB):(512*kiB)]) {
		t.Fatalf("expected output to be equal to input")
	}
}
