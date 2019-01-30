package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"os"

	"../lib/btreedb5"
)

func main() {
	var in string
	flag.StringVar(&in, "i", "input", "input file")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	h, e := btreedb5.Load(in)
	if e != nil {
		log.Fatalln(e)
	}
	defer h.Close()

	h.SetTree(1)
	e = h.Traverse(func(record btreedb5.Record) error {
		z, e := zlib.NewReader(bytes.NewReader(record.Data))
		if e != nil {
			return e
		}

		f, e := os.OpenFile("tree1_"+hex.EncodeToString(record.Key), os.O_RDWR|os.O_CREATE, 0644)
		if e != nil {
			return e
		}

		io.Copy(f, z)

		z.Close()
		f.Close()
		return nil
	})
	if e != nil {
		log.Fatalln(e)
	}

	h.SetTree(2)
	e = h.Traverse(func(record btreedb5.Record) error {
		z, e := zlib.NewReader(bytes.NewReader(record.Data))
		if e != nil {
			return e
		}

		f, e := os.OpenFile("tree2_"+hex.EncodeToString(record.Key), os.O_RDWR|os.O_CREATE, 0644)
		if e != nil {
			return e
		}

		io.Copy(f, z)
		z.Close()
		f.Close()
		return nil
	})
	if e != nil {
		log.Fatalln(e)
	}
}
