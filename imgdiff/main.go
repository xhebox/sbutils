package main

import (
	"fmt"
	"image"
	"image/png"
	"log"
	"os"
	"strings"
)

var (
	images []image.Image
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %s 1.png 2.png ...\n", os.Args[1])
		return
	}

	for _, v := range os.Args[1:] {
		f, e := os.Open(v)
		if e != nil {
			log.Fatal(e)
		}

		img, e := png.Decode(f)
		if e != nil {
			log.Fatal(e)
		}

		images = append(images, img)

		f.Close()
	}

	img := images[0]
	size := img.Bounds().Size()
	for k := range images {
		sz := images[k].Bounds().Size()
		if sz.X != size.X || sz.Y != size.Y {
			log.Fatalf("%d-th image is not of the same size as the first image\n", k)
		}
	}

	for k, l := 1, len(images); k < l; k++ {
		str := map[string]string{}
		cur := images[k]

		for x := 0; x < size.X; x++ {
			for y := 0; y < size.Y; y++ {
				cr, cg, cb, ca := cur.At(x, y).RGBA()
				pr, pg, pb, pa := img.At(x, y).RGBA()

				if cr != pr || cg != pg || cb != pb || ca != pa {
					src := fmt.Sprintf("%02X%02X%02X%02X", uint8(pr), uint8(pg), uint8(pb), uint8(pa))
					dst := fmt.Sprintf("%02X%02X%02X%02X", uint8(cr), uint8(cg), uint8(cb), uint8(ca))

					if v, ok := str[src]; !ok {
						str[src] = dst
					} else if v != dst {
						str[v] = dst
					}
				}
			}
		}

		out := &strings.Builder{}
		fmt.Fprintf(out, "#%d: ?replace", k+1)
		for k, v := range str {
			fmt.Fprintf(out, ";%s=%s", k, v)
		}
		fmt.Println(out)
	}
}
