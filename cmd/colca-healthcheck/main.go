package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	url := "http://127.0.0.1/healthz"
	if len(os.Args) == 2 {
		url = os.Args[1]
	} else if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: colca-healthcheck [url]")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "HTTP %d\n", response.StatusCode)
		os.Exit(1)
	}
}
