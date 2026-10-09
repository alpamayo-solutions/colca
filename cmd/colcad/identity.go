package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
)

// field is one line of a subcommand's output: "name: value", or a JSON key.
type field struct{ name, value string }

func printFields(w io.Writer, asJSON bool, fields []field) error {
	if asJSON {
		m := make(map[string]string, len(fields))
		for _, f := range fields {
			m[f.name] = f.value
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(m)
	}
	for _, f := range fields {
		if _, err := fmt.Fprintf(w, "%s: %s\n", f.name, f.value); err != nil {
			return err
		}
	}
	return nil
}

// identityCmd prints the node's key as the node config names it: what a
// person compares when the node asks its parent to be accepted. It never
// mints a key; a node that has not started yet has none.
func identityCmd(args []string, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("identity", flag.ContinueOnError)
	fl.SetOutput(stderr)
	asJSON := fl.Bool("json", false, "print a JSON object")
	if err := fl.Parse(args); err != nil || fl.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: colcad identity [-json] <config.yaml>")
		return 2
	}
	cfg, err := config.Load(fl.Arg(0))
	if err != nil {
		fmt.Fprintln(stderr, "config:", err)
		return 1
	}
	o := cfg.IdentityOptions()
	id, err := identity.OpenExisting(o)
	if errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(stderr, "no node key at %s yet: colcad creates it on its first start\n", o.KeyFile)
		return 1
	}
	if err != nil {
		fmt.Fprintln(stderr, "identity:", err)
		return 1
	}
	defer func() { _ = id.Close() }()
	if err := printFields(stdout, *asJSON, []field{
		{"ulid", cfg.ULID},
		{"key_store", id.Store},
		{"algorithm", id.Algorithm()},
		{"fingerprint", id.Fingerprint()},
		{"short_fingerprint", id.ShortFingerprint()},
		{"fingerprint_id", id.FingerprintID()},
		{"pubkey", id.PublicHex()},
	}); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// tpmIdentityCmd prints the fingerprint of the TPM's endorsement key, which
// names the chip in a pre-approval. It needs no node config and creates
// nothing persistent, so it can run at staging before colca was ever started.
func tpmIdentityCmd(args []string, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("tpm-identity", flag.ContinueOnError)
	fl.SetOutput(stderr)
	asJSON := fl.Bool("json", false, "print a JSON object")
	device := fl.String("device", identity.DefaultTPMDevice, "TPM device or simulator socket")
	if err := fl.Parse(args); err != nil || fl.NArg() != 0 {
		fmt.Fprintln(stderr, "usage: colcad tpm-identity [-json] [-device /dev/tpmrm0]")
		return 2
	}
	dev, err := tpmkey.Open(*device)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer func() { _ = dev.Close() }()
	ek, err := tpmattest.ReadEK(dev)
	if err != nil {
		fmt.Fprintln(stderr, "endorsement key:", err)
		return 1
	}
	fp := ek.Fingerprint()
	id, _ := pubkey.ParseFingerprint(fp)
	fields := []field{
		{"ek_fingerprint", fp},
		{"ek_short_fingerprint", pubkey.Short(fp)},
		{"ek_fingerprint_id", id},
		{"ek_type", ek.Type},
		{"ek_certificate", "none"},
	}
	if c, err := ek.Certificate(); err != nil {
		fields[4].value = "unparseable: " + err.Error()
	} else if c != nil {
		fields[4].value = "present"
		fields = append(fields, field{"ek_issuer", c.Issuer.String()}, field{"ek_serial", c.SerialNumber.String()})
	}
	if err := printFields(stdout, *asJSON, fields); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
