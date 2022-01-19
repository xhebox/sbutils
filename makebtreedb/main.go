package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/xhebox/bstruct/byteorder"
	"github.com/xhebox/sbutils/lib/btreedb5"
	"github.com/xhebox/sbutils/lib/data_types"
	"github.com/xhebox/sbutils/lib/sbvj01"
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
	if Exists(in) {
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

	for _, v := range files {
		fname := v.Name()

		f, e := os.Open(filepath.Join(dir, fname))
		if e != nil {
			log.Fatalln(e)
		}

		fc, e := ioutil.ReadAll(f)
		if e != nil {
			log.Fatalln(e)
		}

		buf := &bytes.Buffer{}
		zw, e := zlib.NewWriterLevel(buf, zlib.BestCompression)
		if e != nil {
			log.Fatalln(e)
		}

		key := make(btreedb5.Key, h.KeySize)

		switch {
		case fname == "metadata":
			content := map[string]interface{}{}

			e := json.Unmarshal(fc, &content)
			if e != nil {
				log.Fatalln(e)
			}

			size := content["size"].([]interface{})

			e = byteorder.PutUint32(zw, byteorder.BigEndian, uint32(size[0].(float64)))
			if e != nil {
				log.Fatalln(e)
			}

			e = byteorder.PutUint32(zw, byteorder.BigEndian, uint32(size[1].(float64)))
			if e != nil {
				log.Fatalln(e)
			}

			hdr := content["hdr"].(map[string]interface{})

			e = sbvj01.WriteHdr(zw, sbvj01.VerJsonHdr{
				Id:        data_types.String(hdr["id"].(string)),
				Versioned: hdr["versioned"].(bool),
				Version:   int32(uint32(hdr["version"].(float64))),
			})
			if e != nil {
				log.Fatalln(e)
			}

			e = sbvj01.Write(zw, content["body"])
			if e != nil {
				log.Fatalln(e)
			}
		case strings.HasPrefix(fname, "type2_"):
			content := []interface{}{}

			e := json.Unmarshal(fc, &content)
			if e != nil {
				log.Fatalln(e)
			}

			e = byteorder.PutUVarint(zw, byteorder.BigEndian, uint64(uint(len(content))))
			if e != nil {
				log.Fatalln(e)
			}

			for k := range content {
				ii := content[k].(map[string]interface{})

				hdr := ii["hdr"].(map[string]interface{})

				e = sbvj01.WriteHdr(zw, sbvj01.VerJsonHdr{
					Id:        data_types.String(hdr["id"].(string)),
					Versioned: hdr["versioned"].(bool),
					Version:   int32(uint32(hdr["version"].(float64))),
				})

				e = sbvj01.Write(zw, ii["body"])
				if e != nil {
					log.Fatalln(e)
				}
			}
		default:
			zw.Write(fc)

			key, e = hex.DecodeString(fname[5:])
			if e != nil {
				log.Fatalln(e)
			}

			if len(key) != h.KeySize {
				log.Fatalf("key size is not %d\n", h.KeySize)
			}
		}

		zw.Close()
		f.Close()

		e = h.Insert(key, buf.Bytes())
		if e != nil {
			log.Fatalf("%+v\n", e)
		}

		h.Commit()
	}
}
