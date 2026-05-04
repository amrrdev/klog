package main

import (
	"fmt"
	"log"

	"github.com/amrrdev/wal/kv"
)

func main() {
	kv, err := kv.OpenKVStore("./wal-data")
	if err != nil {
		log.Fatal(err)
	}
	defer kv.Close()

	// kv.Set("user:1", "amr")
	// kv.Set("user:2", "ahmed")
	// kv.Set("user:3", "malak")

	// kv.Delete("user:2")

	v, ok := kv.Get("user:1")
	fmt.Printf("user:1 = %q, found = %v\n", v, ok)

	_, ok = kv.Get("user:2")
	fmt.Printf("user:2 found = %v\n", ok)

}
