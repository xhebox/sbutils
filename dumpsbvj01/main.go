package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/xhebox/bstruct/byteorder"
	"github.com/xhebox/sbutils/lib/sbvj01"
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

	rd := bytes.NewReader(contents)

	switch mode {
	case "raw":
		r, e := sbvj01.Read(rd, byteorder.BigEndian)
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
		r, e := sbvj01.ReadHdr(rd)
		if e != nil {
			log.Fatalln(e)
		}

		b, e := sbvj01.Read(rd, r.Endian)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(b, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		_, e = io.Copy(outwt, bytes.NewReader(out))
		if e != nil {
			log.Fatalln(e)
		}
	case "nvj":
		r := int(int8(contents[0]))
		rd = bytes.NewReader(contents[1:])

		for i := 0; i < r; i++ {
			r, e := sbvj01.ReadHdr(rd)
			if e != nil {
				log.Fatalln(e)
			}

			b, e := sbvj01.Read(rd, r.Endian)
			if e != nil {
				log.Fatalln(e)
			}

			out, e := json.MarshalIndent(b, "", "\t")
			if e != nil {
				log.Fatalln(e)
			}

			_, e = io.Copy(outwt, bytes.NewReader(out))
			if e != nil {
				log.Fatalln(e)
			}
		}
	}
}
