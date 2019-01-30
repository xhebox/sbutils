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
	var in, mode string
	var skip int
	flag.StringVar(&in, "i", "input", "input file")
	flag.StringVar(&mode, "m", "vj", "vjmagic/vj/raw")
	flag.IntVar(&skip, "n", 0, "skip first n bytes")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	contents, e := ioutil.ReadFile(in)
	if e != nil {
		log.Fatalln(e)
	}

	contents = contents[skip:]
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

		io.Copy(os.Stdout, bytes.NewReader(out))
	case "vj":
		r, e := sbvj01.Parse(contents)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(r, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		io.Copy(os.Stdout, bytes.NewReader(out))
	case "vjmagic":
		r, e := sbvj01.ParseMagic(contents)
		if e != nil {
			log.Fatalln(e)
		}

		out, e := json.MarshalIndent(r, "", "\t")
		if e != nil {
			log.Fatalln(e)
		}

		io.Copy(os.Stdout, bytes.NewReader(out))
	}
}
