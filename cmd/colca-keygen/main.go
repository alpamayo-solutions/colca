// Command colca-keygen generates an ed25519 identity key and prints its public
// key in hex, the value an admin enrolls at a node.
//
// With -cert it also writes <out.key>.crt, a self-signed certificate wrapping
// the key, for MQTT and HTTPS clients that need a TLS client certificate. Trust
// comes from pinning the key; there is no CA.
package main

import (
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/alpamayo-solutions/colca/internal/identity"
)

func main() {
	writeCert := flag.Bool("cert", false, "also write <out.key>.crt (PEM self-signed cert wrapping the key)")
	ifMissing := flag.Bool("if-missing", false, "load an existing key or generate it once; never rotate it")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: colca-keygen [-cert] [-if-missing] <out.key>  (prints pubkey hex to stdout)")
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	keyPath := flag.Arg(0)
	id, err := generate(keyPath, *ifMissing)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *writeCert {
		cn := filepath.Base(keyPath)
		cert, err := id.SelfSignedCert(cn)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
		if err := os.WriteFile(keyPath+".crt", pemBytes, 0o644); err != nil { // #nosec G306 -- a certificate is public
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println(id.PublicHex())
}

func generate(keyPath string, ifMissing bool) (*identity.Identity, error) {
	if ifMissing {
		id, _, err := identity.LoadOrGenerate(keyPath)
		return id, err
	}
	return identity.Generate(keyPath)
}
