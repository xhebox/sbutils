package main

import (
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
	flag.StringVar(&in, "i", "input", "input json")
	flag.StringVar(&out, "o", "stdout", "output versioned json")
	flag.StringVar(&mode, "m", "vj", "vjmagic/vj/raw")
	flag.Parse()
	log.SetFlags(log.Llongfile)

	contents, e := ioutil.ReadFile(in)
	if e != nil {
		log.Fatalln(e)
	}

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
		var r interface{}

		if e := json.Unmarshal(contents, r); e != nil {
			log.Fatalln(e)
		}

		if e := sbvj01.WriteRaw(outwt, r, byteorder.BigEndian); e != nil {
			log.Fatalln(e)
		}
	case "vj":
		var r interface{}

		if e := json.Unmarshal(contents, &r); e != nil {
			log.Fatalln(e)
		}

		v, ok := r.(map[string]interface{})
		if !ok {
			log.Fatalln("not a versioned json?")
		}

		vj := sbvj01.VersionedJson{Id: v["Id"].(string), Version: int(v["Version"].(float64)), Content: v["Content"]}

		if e := sbvj01.Write(outwt, vj); e != nil {
			log.Fatalln(e)
		}
	case "vjmagic":
		var r interface{}

		if e := json.Unmarshal(contents, &r); e != nil {
			log.Fatalln(e)
		}

		v, ok := r.(map[string]interface{})
		if !ok {
			log.Fatalln("not a versioned json?")
		}

		vj := sbvj01.VersionedJson{Id: v["Id"].(string), Version: int(v["Version"].(float64)), Content: v["Content"]}

		if e := sbvj01.WriteMagic(outwt, vj); e != nil {
			log.Fatalln(e)
		}
	}
}
