package main

import (
	"fmt"
	"os"

	"github.com/alpamayo-solutions/colca/internal/identity"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: colca-keygen <out.key>  (prints pubkey hex to stdout)")
		os.Exit(2)
	}
	id, err := identity.Generate(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(id.PublicHex())
}
