package simplefs

import (
	"bytes"
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

	err := cbuf.InjectChunk(3, []byte{0x03, 0x04, 0x05, 0x06})
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

	err := cbuf.InjectChunk(5, []byte{0x05, 0x06, 0x07, 0x08})
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

	err := cbuf.InjectChunk(3, []byte{0x03, 0x04, 0x05, 0x06})
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

	err := cbuf.InjectChunk(0, []byte{0x00, 0x01, 0x02, 0x03, 0x04})
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

	err := cbuf.InjectChunk(0, []byte{0x00, 0x01, 0x02, 0x03})
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

	err := cbuf.InjectChunk(2, []byte{0x88, 0x88})
	if err != nil {
		t.Error(err)
	}

	result := []byte{}
	for _, c := range cbuf.chunks {
		result = append(result, c.d...)
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

	if !bytes.Equal(result, []byte{0x00, 0x01, 0x88, 0x88, 0x04, 0x05}) {
		t.Errorf("expected stitching to not corrupt data, result: %v", result)
	}

}
