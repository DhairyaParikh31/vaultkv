// Command cli is an interactive command-line client for VaultKV.
//
// Usage:
//
//	vaultkv-cli --addr localhost:6380
//
// Once connected, type commands at the prompt:
//
//	> SET name alice
//	+OK
//	> GET name
//	+alice
//	> DEL name
//	+OK
//	> PING
//	+PONG
//	> STAT
//	+{...}
//	> exit
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "localhost:6380", "VaultKV server address")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to VaultKV at %s\nType SET/GET/DEL/PING/STAT or 'exit' to quit.\n\n", *addr)

	stdin := bufio.NewScanner(os.Stdin)
	serverReader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")

		if !stdin.Scan() {
			break
		}

		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}
		if strings.ToLower(line) == "exit" || strings.ToLower(line) == "quit" {
			fmt.Println("Bye.")
			break
		}

		// Send command to server.
		fmt.Fprintf(conn, "%s\r\n", line)

		// Read response.
		resp, err := serverReader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "read response: %v\n", err)
			break
		}

		resp = strings.TrimRight(resp, "\r\n")
		fmt.Println(resp)
	}
}