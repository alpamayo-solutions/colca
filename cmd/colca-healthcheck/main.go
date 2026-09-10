// Command colca-healthcheck exits 0 when a URL answers 200 OK, for container health checks.
package main

import (
	"context"
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
	if err := check(url); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(url string) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", response.StatusCode)
	}
	return nil
}
