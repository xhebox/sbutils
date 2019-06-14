package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"

	"../lib/sbvj01"
	"github.com/xhebox/bstruct/byteorder"
)

func main() {
	var in, out, mode string
	var skip int
	flag.StringVar(&in, "i", "input", "versioned json file")
	flag.StringVar(&out, "o", "stdout", "output json")
	flag.StringVar(&mode, "m", "vj", "vjmagic/vj/raw/nvj")
	flag.IntVar(&skip, "n", 0, "skip first n bytes")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	contents, e := ioutil.ReadFile(in)
	if e != nil {
		log.Fatalln(e)
	}

	contents = contents[skip:]

	var outwt io.Writer
	if out == "stdout" {
		outwt = os.Stdout
	} else {
		f, e := os.OpenFile(out, os.O_CREATE|os.O_RDWR, 0644)
		if e != nil {
			log.Fatalln(e)
		}
		defer f.Close()

		outwt = f
	}

	switch mode {
	case "raw":
		r, _, e := sbvj01.ParseRaw(contents, byteorder.BigEndian)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(r, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		_, e = io.Copy(outwt, bytes.NewReader(out))
		if e != nil {
			log.Fatalln(e)
		}
	case "vj":
		r, _, e := sbvj01.Parse(contents)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(r, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		_, e = io.Copy(outwt, bytes.NewReader(out))
		if e != nil {
			log.Fatalln(e)
		}
	case "nvj":
		r := int(int8(contents[0]))

		off := 1
		for i := 0; i < r; i++ {
			r, l, e := sbvj01.Parse(contents[off:])
			if e != nil {
				log.Fatalln(e)
			}

			out, e := json.MarshalIndent(r, "", "\t")
			if e != nil {
				log.Fatalln(e)
			}

			_, e = io.Copy(outwt, bytes.NewReader(out))
			if e != nil {
				log.Fatalln(e)
			}

			off += l
		}
	case "vjmagic":
		r, _, e := sbvj01.ParseMagic(contents)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(r, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		_, e = io.Copy(outwt, bytes.NewReader(out))
		if e != nil {
			log.Fatalln(e)
		}
	}
}
