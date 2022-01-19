package main

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/xhebox/bstruct/byteorder"
	"github.com/xhebox/sbutils/lib/btreedb5"
	"github.com/xhebox/sbutils/lib/sbvj01"
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
	default:
		e = h.Ascend(func(key btreedb5.Key, data []byte) {
			z, e := zlib.NewReader(bytes.NewReader(data))
			if e != nil {
				log.Fatalln(e)
			}

			switch key[0] {
			case 0:
				f, e := os.OpenFile("metadata", os.O_RDWR|os.O_CREATE, 0644)
				if e != nil {
					log.Fatalln(e)
				}

				x, e := byteorder.Uint32(z, byteorder.BigEndian)
				if e != nil {
					log.Fatalln(e)
				}

				y, e := byteorder.Uint32(z, byteorder.BigEndian)
				if e != nil {
					log.Fatalln(e)
				}

				hdr, e := sbvj01.ReadHdr(z)
				if e != nil {
					log.Fatalln(e)
				}

				body, e := sbvj01.Read(z)
				if e != nil {
					log.Fatalln(e)
				}

				json, e := json.MarshalIndent(map[string]interface{}{
					"size": []uint32{x, y},
					"hdr":  hdr,
					"body": body,
				}, "", "\t")
				if e != nil {
					log.Fatalln(e)
				}

				f.Write(json)

				f.Close()
			case 2:
				f, e := os.OpenFile(fmt.Sprintf("type2_%s", hex.EncodeToString(key[1:])), os.O_RDWR|os.O_CREATE, 0644)
				if e != nil {
					log.Fatalln(e)
				}

				cnt, e := byteorder.UVarint(z, byteorder.BigEndian)
				if e != nil {
					log.Fatalln(e)
				}

				vjs := []map[string]interface{}{}

				for i, j := 0, int(cnt); i < j; i++ {
					hdr, e := sbvj01.ReadHdr(z)
					if e != nil {
						log.Fatalln(e)
					}

					body, e := sbvj01.Read(z)
					if e != nil {
						log.Fatalln(e)
					}

					vjs = append(vjs, map[string]interface{}{
						"hdr":  hdr,
						"body": body,
					})
				}

				json, e := json.MarshalIndent(vjs, "", "\t")
				if e != nil {
					fmt.Printf("%v %+v\n", e, vjs)
					log.Fatalln(e)
				}

				f.Write(json)

				f.Close()
			default:
				f, e := os.OpenFile(fmt.Sprintf("data_%s", hex.EncodeToString(key)), os.O_RDWR|os.O_CREATE, 0644)
				if e != nil {
					log.Fatalln(e)
				}

				io.Copy(f, z)

				f.Close()
			}

			z.Close()
		})
		if e != nil {
			log.Fatalf("%+v\n", e)
		}
	}
}
