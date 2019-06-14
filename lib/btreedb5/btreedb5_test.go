package btreedb5

/*
func TestApi(b *testing.T) {
	h, e := New("test2", "fuck", 512, 5)
	if e != nil {
		b.Fatal(e)
	}
	h.Close()

	h, e = Load("test2")
	if e != nil {
		b.Fatal(e)
	}
	defer h.Close()

	for k := 0; k < 1000000; k++ {
		j1 := byte(k >> 24)
		j2 := byte(k >> 16)
		j3 := byte(k >> 8)
		j4 := byte(k)

		e = h.Insert([]byte{j1, j2, j3, j4, 5}, []byte{1, 2, 4, 5, 6})
		if e != nil {
			fmt.Println(k)
			b.Fatal(e)
		}
	}

	for k := 0; k < 999999; k++ {
		j1 := byte(k >> 24)
		j2 := byte(k >> 16)
		j3 := byte(k >> 8)
		j4 := byte(k)

		e = h.Delete([]byte{j1, j2, j3, j4, 5})
		if e != nil {
			b.Fatal(e)
		}
	}
}
*/
