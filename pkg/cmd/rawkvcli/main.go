package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"google.golang.org/grpc"

	"github.com/chuyangliu/rawkv/pkg/pb"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Missing command.")
		printUsage()
		os.Exit(1)
	}

	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	getAddr := getCmd.String("addr", "127.0.0.1:5640", "Server address to connect.")
	getKey := getCmd.String("key", "", "Key to get.")

	putCmd := flag.NewFlagSet("put", flag.ExitOnError)
	putAddr := putCmd.String("addr", "127.0.0.1:5640", "Server address to connect.")
	putKey := putCmd.String("key", "", "Key to put.")
	putVal := putCmd.String("val", "", "Value to put.")

	delCmd := flag.NewFlagSet("del", flag.ExitOnError)
	delAddr := delCmd.String("addr", "127.0.0.1:5640", "Server address to connect.")
	delKey := delCmd.String("key", "", "Key to delete.")

	switch os.Args[1] {
	case "get":
		getCmd.Parse(os.Args[2:])
		execGet(*getAddr, *getKey)
	case "put":
		putCmd.Parse(os.Args[2:])
		execPut(*putAddr, *putKey, *putVal)
	case "del":
		delCmd.Parse(os.Args[2:])
		execDel(*delAddr, *delKey)
	default:
		fmt.Printf("Unrecognized command \"%v\"\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("\nUsage:")
	fmt.Printf("\n\t%v <command> [arguments]\n", os.Args[0])
	fmt.Println("\nThe commands are:")
	fmt.Println("\n\tget\tget the value associated with a key")
	fmt.Println("\tput\tput or update a key-value pair")
	fmt.Println("\tdel\tdelete a key")
	fmt.Println("")
}

func execGet(addr string, key string) {
	fmt.Printf("[Get %v]\nKey=\"%v\"\n", addr, key)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewStorageClient(conn)

	req := &pb.GetReq{Key: []byte(key)}
	resp, err := client.Get(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Value=\"%v\"\nFound=%v\n", string(resp.Val), resp.Found)
}

func execPut(addr string, key string, val string) {
	fmt.Printf("[Put %v]\nKey=\"%v\"\nValue=\"%v\"\n", addr, key, val)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewStorageClient(conn)

	req := &pb.PutReq{Key: []byte(key), Val: []byte(val)}
	_, err = client.Put(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Success")
}

func execDel(addr string, key string) {
	fmt.Printf("[Del %v]\nKey=\"%v\"\n", addr, key)

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewStorageClient(conn)

	req := &pb.DelReq{Key: []byte(key)}
	_, err = client.Del(context.Background(), req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Success")
}
