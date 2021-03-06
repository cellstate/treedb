package simplefs

import (
	"fmt"
	"io"

	"github.com/restic/chunker"
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

//A ChunkBuf provides a malleable in-memory slice of chunks
type ChunkBuf struct {
	pos uint64
	pw  io.WriteCloser
	pol chunker.Pol

	flushCh chan chan error
	chunks  []*chunk
}

//NewChunkBuf creates a chunked file interface
func NewChunkBuf() (*ChunkBuf, error) {
	buf := &ChunkBuf{
		pol:     chunker.Pol(0x3DA3358B4DC173),
		flushCh: make(chan chan error),
		chunks:  []*chunk{{o: 0, eof: true}},
	}

	//chunking injects new chunks into the chunk slice as they are produced
	chunking := func(off uint64, chkr *chunker.Chunker, doneCh chan<- error) {
		b := make([]byte, chkr.MaxSize)
		var doneErr error
		for {
			chunk, err := chkr.Next(b)
			if err == io.EOF {
				break
			}

			d := make([]byte, chunk.Length)
			copy(d, chunk.Data)

			fmt.Println("inject:", chunk.Start, len(d))
			err = buf.inject(off+uint64(chunk.Start), d)
			if err != nil {
				doneErr = err
				break
			}
		}

		//signal all chunks have been injected
		doneCh <- doneErr
	}

	//routine for handling flush requests.
	go func() {
		doneCh := make(chan error)
		for freq := range buf.flushCh {

			//flush the last chunker
			if buf.pw != nil {
				err := buf.pw.Close()
				if err != nil {
					freq <- err //failed to flush
					continue
				}

				err = <-doneCh
				if err != nil {
					freq <- err //failed to flush
					continue
				}
			}

			//setup a new chunking pipe
			var pr io.Reader
			pr, buf.pw = io.Pipe()
			chunker := chunker.NewWithBoundaries(
				pr, buf.pol, (256 * kiB), (1 * miB),
			)

			//from current file position start chunking, we'll send something on doneCh when done
			go chunking(buf.pos, chunker, doneCh)

			//respond to flush, all OK
			freq <- nil
		}
	}()

	//initial flush to set stuff up
	err := buf.flush()
	if err != nil {
		return nil, fmt.Errorf("failed to setup: %v", err)
	}

	return buf, nil
}

//flush will close the chunk writer. This will cause the chunker to turn any remaining (buffered) bytes into a last chunk before starting a new one. A new chunker is started at the current cursor position
func (buf *ChunkBuf) flush() error {
	freq := make(chan error)
	buf.flushCh <- freq
	err := <-freq
	if err != nil {
		return err
	}

	return nil
}

//inject takes new chunks produced by the Chunker and positions them correctly in-line without losing bytes, it uses the new chunk's left position (offset) and right
func (buf *ChunkBuf) inject(offset uint64, data []byte) error {
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
				eofC.o = end //shift EOF chunk
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

//Seek will moves the current position to absolute position 'pos', in doing so it flushes currently buffered data from the chunker into a new chunk by closing the underlying writer. A new writer is started and the chunker is reset.
func (buf *ChunkBuf) Seek(pos uint64) error {

	//@TODO dont reset if chunker is already positioned correctly

	return ErrNotImplemented
}

//Write will push bytes into the chunker, the chunker may buffer bytes util it has reached it maxed size, this buffer if flushed or the writer is closed. Writing takes place at the current file offset, and the file offset is incremented by the number of bytes actually written.
func (buf *ChunkBuf) Write(b []byte) (n int, err error) {
	n, err = buf.pw.Write(b)
	buf.pos += uint64(n)
	return n, err
}
