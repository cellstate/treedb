package simplefs

// func TestWrite(t *testing.T) {
// 	fs, close := testfs(t)
// 	defer close()
//
// 	f, err := fs.OpenFile(P{"foo.txt"}, os.O_CREATE, 0777)
// 	if err != nil {
// 		t.Fatalf("didn't expect error, got: %v", err)
// 	}
//
// 	fmt.Printf("chunker max: %d KiB, min: %d KiB\n", (f.chkr.MaxSize / kiB), (f.chkr.MinSize / kiB))
//
// 	input := make([]byte, 2*miB)
// 	rand.Read(input)
//
// 	n, err := f.Write(input)
// 	if err != nil {
// 		t.Fatalf("didn't expect error, got: %v", err)
// 	}
//
// 	if n != len(input) {
// 		t.Errorf("expected 3 bytes to be written, got: %d", n)
// 	}
//
// 	//close will trigger chunkers io.EOF behaviour: writing any remaining bytes in its window buffer to our own buffer. writing this buffer runs in a different go-routine though so the buffer might be, or might not be written upon printing it here
// 	err = f.Pw.Close()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	time.Sleep(time.Millisecond * 10)
// 	fmt.Println("written bytes:", n)
// 	total := 0
// 	for offset, chunk := range f.chunks {
// 		total = total + len(chunk)
// 		fmt.Printf("chunk@%d, size: %d KiB\n", offset, len(chunk)/kiB)
// 	}
//
// 	if total != n {
// 		t.Error("expected same amount in chunked bytes as was written")
// 	}
//
// 	fmt.Println("chunked bytes:", total)
//
// }
