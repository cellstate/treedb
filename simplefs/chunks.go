package simplefs

import (
	"fmt"
	"io"
)

type chunk struct {
	o   uint64 //absolute offset in the file that is chunked
	d   []byte //this might or might not have been flushed to the db
	eof bool   //a eof chunk never has data and marks the end of the chunk slice
}

func (c chunk) data() ([]byte, error) {
	if c.d == nil { //@TODO sometimes needs to be fetched lazily from the db
		return nil, ErrNotImplemented
	}

	return c.d, nil
}

//A ChunkBuf provides a malleable in-memory map of chunks
type ChunkBuf struct {
	pos    uint64
	chunks []*chunk
}

//InjectChunk takes new chunks produced by the Chunker and positions them correctly in-line without losing bytes, it uses the new chunk's left position (offset) and right
func (buf *ChunkBuf) InjectChunk(offset uint64, data []byte) error {
	var injected bool

	//here we walk over existing chunks and filter what is being transferred to a new chunk slice that uses the same unerlying array to prevent allocations
	nchunks := buf.chunks[:0]
	end := offset + uint64(len(data))
	for i, c := range buf.chunks {
		if c.eof {

			//if the chunk was not yet injected when reaching EOF we assume it should be placed here. The EOF chunk is placed after the final chunk
			eofC := &chunk{o: c.o, eof: true}
			if injected == false {
				nchunks = append(nchunks, &chunk{o: offset, d: data})
				injected = true
				eofC.o = end
			}

			nchunks = append(nchunks, eofC) //add EOF chunk
			break
		}

		if (len(buf.chunks) - 1) < i+1 {
			return io.ErrUnexpectedEOF //no EOF chunk
		}

		left := c.o
		right := buf.chunks[i+1].o
		if offset >= right || left > end {
			//new chunk doesnt touch these existing chunks at all, just copy it over
		} else if offset >= left && offset < right {

			//new chunk starts here, only move over left most part
			startData, err := c.data()
			if err != nil {
				return err
			}

			startDelta := offset - left
			if startDelta == 0 {
				//no data left of the original chunk skip it entirely
				continue
			}

			c.d = startData[:startDelta]
		} else if end >= left && end < right {

			//new chunk ends here. insert it
			nchunks = append(nchunks, &chunk{o: offset, d: data})
			injected = true

			//copy over only the right part for the existing chunk and shift offset
			endData, err := c.data()
			if err != nil {
				return err
			}

			endDelta := end - left
			c.d = endData[endDelta:]
			c.o = c.o + endDelta
		} else {

			//chunk completely overwites it, dont copy it over
			continue
		}

		//place chunk in new version
		nchunks = append(nchunks, c)
	}

	if injected == false {
		return fmt.Errorf("failed to inject chunk with offset %d", offset)
	}

	//at last, replace the existing chunk slice with the new one
	buf.chunks = nchunks
	return nil
}

//Seek will moves the current position to absolute position 'pos', in doing so it flushes currently buffered data from the chunker into a new chunk by closing the underlying writer. A new writer is started and the chunker is reset. If the new position is halfway a old chunk, the part of the chunk in front of this position is immediately written to the chunker.
func (buf *ChunkBuf) Seek(pos uint64) error {

	//@TODO dont reset if chunker is already positioned correctly

	return ErrNotImplemented
}

//Write will push bytes into the chunker
func (buf *ChunkBuf) Write(b []byte) error {
	return nil
}
