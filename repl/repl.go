package main

import (
	"bufio"
	"com/github/tomboyo/lsf/db"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanWords)

	datadir, err := os.UserHomeDir()
	if err != nil {
		fmt.Println("Could not find home directory!")
		return
	}

	datadir = filepath.Join(datadir, ".lfsdb")
	os.MkdirAll(datadir, 0600)

	db := db.NewDb(datadir)

	fmt.Println("Ready for commands")

	for hasNext := scanner.Scan(); hasNext; hasNext = scanner.Scan() {
		token := scanner.Text()

		if token == "add" {
			if !scanner.Scan() {
				fmt.Printf("Usage: add <key> <value>")
			}
			key := scanner.Text()

			if !scanner.Scan() {
				fmt.Printf("Usage: add <key> <value>")
			}
			value := scanner.Text()

			db.Add(key, value)
			fmt.Printf("Persisted %s = %s\n", key, value)
		} else if token == "get" {
			if !scanner.Scan() {
				fmt.Printf("Usage: get <key>")
			}
			key := scanner.Text()

			value, err := db.Get(key)
			if err {
				fmt.Println(*value)
			} else {
				fmt.Printf("Value not found for key %s\n", key)
			}
		} else {
			fmt.Printf("Unexpected token %q\n", token)
		}
	}
}
