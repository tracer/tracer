package main

import (
	"fmt"

	"github.com/boltdb/bolt"
)

func main() {
	db, err := bolt.Open("/tmp/db", 0600, nil)
	if err != nil {
		panic(err)
	}
	db.View(func(tx *bolt.Tx) error {
		fmt.Println("indexes:")
		indexes := tx.Bucket([]byte("indexes"))
		c := indexes.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("span id=%s, full id=%s\n", k, v)
		}
		fmt.Println()

		fmt.Println("spans:")
		spans := tx.Bucket([]byte("spans"))
		c = spans.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}

		return nil
	})

}
