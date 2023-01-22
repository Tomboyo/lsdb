package main

import (
	"bufio"
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

			fmt.Printf("Echo: asked to persist %s = %s\n", key, value)
		} else if token == "get" {
			if !scanner.Scan() {
				fmt.Printf("Usage: get <key>")
			}
			key := scanner.Text()

			fmt.Printf("Echo: asked to get %s\n", key)
		} else {
			fmt.Printf("Unexpected token %q\n", token)
		}
	}
}
