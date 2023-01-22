package main

import (
	"bufio"
	"com/github/tomboyo/lsf/db"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanWords)

	fmt.Println("Begin")

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

			err := db.Add(key, value)
			if err != nil {
				fmt.Printf("Could not persist %s = %s\n\t%v\n", key, value, err)
			} else {
				fmt.Printf("Persisted %s = %s\n", key, value)
			}
		} else if token == "get" {
			if !scanner.Scan() {
				fmt.Printf("Usage: get <key>")
			}
			key := scanner.Text()

			value, err := db.Get(key)
			if err == nil {
				fmt.Println(value)
			} else {
				fmt.Printf("Could not get value for %s:\n\t%v\n", key, err)
			}
		} else {
			fmt.Printf("Unexpected token %q\n", token)
		}
	}
}
