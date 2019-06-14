package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/xhebox/sbutils/lib/btreedb5"
)

func Exists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func main() {
	var in, dir string
	var root bool
	flag.StringVar(&in, "i", "input", "db file")
	flag.StringVar(&dir, "d", "dir", "records dir")
	flag.BoolVar(&root, "r", false, "root")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	var h *btreedb5.BTreeDB5
	var e error
	//if Exists(in) {
	if false {
		h, e = btreedb5.Load(in)
		if e != nil {
			log.Fatalln(e)
		}
	} else {
		h, e = btreedb5.New(in, "World4", 2048, 5)
		if e != nil {
			log.Fatalln(e)
		}
	}
	defer h.Close()

	files, e := ioutil.ReadDir(dir)
	if e != nil {
		log.Fatalln(e)
	}

	keys := [][]byte{}

	for _, v := range files {
		fname := v.Name()

		f, e := os.Open(filepath.Join(dir, fname))
		if e != nil {
			log.Fatalln(e)
		}

		buf := &bytes.Buffer{}
		zw, e := zlib.NewWriterLevel(buf, zlib.BestCompression)
		if e != nil {
			log.Fatalln(e)
		}

		if _, e := io.Copy(zw, f); e != nil {
			log.Fatalln(e)
		}

		zw.Close()
		f.Close()

		keyhex := fname[5:]

		key, e := hex.DecodeString(keyhex)
		if e != nil {
			log.Fatalln(e)
		}

		if len(key) != h.KeySize {
			log.Fatalf("key size is not %d\n", h.KeySize)
		}

		keys = append(keys, key)

		e = h.Insert(key, buf.Bytes())
		if e != nil {
			log.Fatalf("%+v\n", e)
		}

		h.Commit()
	}

	le := len(keys) - 1
	for k := range keys {
		fmt.Println("remove:", keys[le-k])
		e = h.Remove(keys[le-k])
		if e != nil {
			log.Fatalf("%+v\n", e)
		}

		h.Commit()
	}
}
