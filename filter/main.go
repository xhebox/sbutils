package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/xhebox/sbutils/lib/packet"
)

func Callback(r *Filter, p packet.Packet) {
	fmt.Printf("%+v\n", p)
}

var laddr, saddr, mode string

func forward(conn net.Conn) {
	rconn, err := net.Dial("tcp", saddr)
	if err != nil {
		log.Fatalln("dial failed: ", err)
	}

	log.Println("connected to: ", rconn.RemoteAddr())
	switch mode {
	case "all":
		p := NewFilter(conn, rconn, Callback)
		q := NewFilter(rconn, conn, Callback)
		go func() {
			defer conn.Close()
			defer rconn.Close()
			e := p.Loop()
			log.Printf("parse cs: %+v\n", e)
		}()
		go func() {
			defer conn.Close()
			defer rconn.Close()
			e := q.Loop()
			log.Printf("parse sc: %+v\n", e)
		}()
	case "client":
		p := NewFilter(conn, rconn, Callback)
		go func() {
			defer conn.Close()
			defer rconn.Close()
			e := p.Loop()
			log.Printf("parse cs: %+v\n", e)
		}()
		go func() {
			io.Copy(conn, rconn)
		}()
	case "server":
		q := NewFilter(rconn, conn, Callback)
		go func() {
			defer conn.Close()
			defer rconn.Close()
			e := q.Loop()
			log.Printf("parse sc: %+v\n", e)
		}()
		go func() {
			io.Copy(rconn, conn)
		}()
	default:
		go func() {
			defer rconn.Close()
			defer conn.Close()
			io.Copy(conn, rconn)
		}()
		go func() {
			defer rconn.Close()
			defer conn.Close()
			io.Copy(rconn, conn)
		}()
	}
}

func main() {
	flag.StringVar(&laddr, "l", "127.0.0.1:21026", "listened address")
	flag.StringVar(&saddr, "t", "127.0.0.1:21025", "server address")
	flag.StringVar(&mode, "m", "all", "could be client(filter packages from client to server)/server(filter packages from server to client)/all")
	flag.Parse()
	log.SetFlags(log.Lshortfile)

	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatalln("failed to listen: ", err)
	}
	log.Println("listened to: ", laddr)
	log.Println("forward to: ", saddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalln("failed to accept connection: ", err)
		}

		log.Println("accepted connection: ", conn.RemoteAddr())
		go forward(conn)
	}
}
