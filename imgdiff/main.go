package main

import (
	"fmt"
	"image"
	"image/color"
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
	imgrect := img.Bounds()
	size := imgrect.Size()
	for k := range images {
		sz := images[k].Bounds().Size()
		if sz.X != size.X || sz.Y != size.Y {
			log.Fatalf("%d-th image is not of the same size as the first image\n", k)
		}
	}

	newimg := image.NewRGBA(imgrect)
	for x := 0; x < size.X; x++ {
		for y := 0; y < size.Y; y++ {
			newimg.Set(x, y, img.At(x, y))
		}
	}

	k1 := 0
	k2 := 0
	k3 := 0
	k4 := 0x41

	for k, l := 1, len(images); k < l; k++ {
		str := map[string]string{}
		cur := images[k]

		for x := 0; x < size.X; x++ {
			for y := 0; y < size.Y; y++ {
				cr, cg, cb, ca := cur.At(x, y).RGBA()
				pr, pg, pb, pa := img.At(x, y).RGBA()
				src := fmt.Sprintf("%02X%02X%02X%02X", uint8(pr), uint8(pg), uint8(pb), uint8(pa))
				dst := ""

				if cr != pr || cg != pg || cb != pb || ca != pa {
					dst = fmt.Sprintf("%02X%02X%02X%02X", uint8(cr), uint8(cg), uint8(cb), uint8(ca))
				} else {
					dst = src
				}

				if v, ok := str[src]; !ok {
					str[src] = dst
				} else if v != dst {
					newimg.SetRGBA(x, y, color.RGBA{uint8(k1), uint8(k2), uint8(k3), uint8(k4)})
					k3++
					if k3 == 256 {
						k3 = 0
						k2++
					}
					if k2 == 256 {
						k2 = 0
						k1++
					}
					if k1 == 256 {
						log.Fatalf("overflowed, can not transform %d-th image even with new images\n", k+1)
					}
				}
			}
		}
	}

	for k, l := 0, len(images); k < l; k++ {
		str := map[string]string{}
		cur := images[k]

		for x := 0; x < size.X; x++ {
			for y := 0; y < size.Y; y++ {
				cr, cg, cb, ca := cur.At(x, y).RGBA()
				pr, pg, pb, pa := newimg.At(x, y).RGBA()

				if cr != pr || cg != pg || cb != pb || ca != pa {
					src := fmt.Sprintf("%02X%02X%02X%02X", uint8(pr), uint8(pg), uint8(pb), uint8(pa))
					dst := fmt.Sprintf("%02X%02X%02X%02X", uint8(cr), uint8(cg), uint8(cb), uint8(ca))

					if _, ok := str[src]; !ok {
						str[src] = dst
					}
				}
			}
		}

		if len(str) != 0 {
			out := &strings.Builder{}
			fmt.Fprintf(out, "#%d: ?replace", k+1)
			for k, v := range str {
				fmt.Fprintf(out, ";%s=%s", k, v)
			}
			fmt.Println(out)
		}
	}

	if k1+k2+k3+k4 != 0x41 {
		f, e := os.OpenFile("new.png", os.O_RDWR|os.O_CREATE, 0644)
		if e != nil {
			log.Fatal(e)
		}
		defer f.Close()

		if e := png.Encode(f, newimg); e != nil {
			log.Fatal(e)
		}
	}
}
