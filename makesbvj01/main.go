package main

import (
	"encoding/json"
	"flag"
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
		var r interface{}

		if e := json.Unmarshal(contents, r); e != nil {
			log.Fatalln(e)
		}

		if e := sbvj01.WriteRaw(os.Stdout, r, byteorder.BigEndian); e != nil {
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

		if e := sbvj01.Write(os.Stdout, vj); e != nil {
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

		if e := sbvj01.WriteMagic(os.Stdout, vj); e != nil {
			log.Fatalln(e)
		}
	}
}
