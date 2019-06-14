package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/xhebox/sbutils/lib/btreedb5"
)

func main() {
	var in, mode string
	flag.StringVar(&in, "i", "input", "input file")
	flag.StringVar(&mode, "m", "default", "default/records")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	h, e := btreedb5.Load(in)
	if e != nil {
		log.Fatalln(e)
	}
	defer h.Close()

	switch mode {
	/*
		case "records":
			e = h.DumpBlocks(func(record btreedb5.Record) error {
				z, e := zlib.NewReader(bytes.NewReader(record.Data))
				if e != nil {
					return e
				}

				f, e := os.OpenFile(hex.EncodeToString(record.Key), os.O_RDWR|os.O_CREATE, 0644)
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
	*/
	default:
		e = h.Ascend(func(key btreedb5.Key, data []byte) {
			z, e := zlib.NewReader(bytes.NewReader(data))
			if e != nil {
				panic(e)
			}

			f, e := os.OpenFile(fmt.Sprintf("data_%s", hex.EncodeToString(key)), os.O_RDWR|os.O_CREATE, 0644)
			if e != nil {
				panic(e)
			}

			io.Copy(f, z)

			z.Close()
			f.Close()
		})
		if e != nil {
			log.Fatalf("%+v\n", e)
		}
	}
}
