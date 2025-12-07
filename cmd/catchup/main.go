package main

import (
	"fmt"
	"net/rpc"
)

const addr = "localhost:5002"

func main() {
	fmt.Printf("Connecting to Gitter at %s...\n", addr)

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	fmt.Println("Connected. Sending CatchUp request...")

	reply := ""
	if err := client.Call("Gitter.CatchUp", struct{}{}, &reply); err != nil {
		panic(err)
	}

	fmt.Printf("Reply: %s\n", reply)
}
