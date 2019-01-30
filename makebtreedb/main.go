package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"../lib/btreedb5"
)

func main() {
	var in, dir string
	flag.StringVar(&in, "i", "input", "db file")
	flag.StringVar(&dir, "d", "dir", "input dir file")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	h, e := btreedb5.Load(in)
	if e != nil {
		log.Fatalln(e)
	}
	defer h.Close()

	files, e := ioutil.ReadDir(dir)
	if e != nil {
		log.Fatalln(e)
	}

	for _, v := range files {
		fname := v.Name()

		f, e := os.Open(filepath.Join(dir, fname))
		if e != nil {
			log.Fatalln(e)
		}

		buf := &bytes.Buffer{}
		zw := zlib.NewWriter(buf)

		if _, e := io.Copy(zw, f); e != nil {
			log.Fatalln(e)
		}

		zw.Close()
		f.Close()

		tree1 := strings.HasPrefix(fname, "tree1")
		keyhex := fname[6:]
		if tree1 {
			h.SetTree(1)
		} else {
			h.SetTree(2)
		}

		key, e := hex.DecodeString(keyhex)
		if e != nil {
			log.Fatalln(e)
		}

		if len(key) != h.KeySize {
			log.Fatalf("key size is not %d\n", h.KeySize)
		}

		h.Insert(key, buf.Bytes())
	}
}
