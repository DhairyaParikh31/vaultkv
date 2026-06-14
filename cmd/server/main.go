// Command server starts the VaultKV TCP server.
//
// Usage:
//
//	vaultkv-server --port 6380 --data ./data --sync full
//
// Flags:
//
//	--port   TCP port to listen on (default: 6380)
//	--data   Directory for WAL and SSTable files (default: ./data)
//	--sync   WAL sync mode: full | batched | none (default: full)
//	--mem    MemTable size in MB before flush (default: 4)
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	vaultkv "github.com/DhairyaParikh31/vaultkv"
)

func main() {
	port := flag.String("port", "6380", "TCP port to listen on")
	dataDir := flag.String("data", "./data", "Data directory for WAL and SSTables")
	syncMode := flag.String("sync", "full", "WAL sync mode: full | batched | none")
	memMB := flag.Int("mem", 4, "MemTable size in MB before flush")
	flag.Parse()

	// Map sync flag to SyncMode.
	var sm vaultkv.SyncMode
	switch *syncMode {
	case "full":
		sm = vaultkv.SyncFull
	case "batched":
		sm = vaultkv.SyncBatched
	case "none":
		sm = vaultkv.SyncNone
	default:
		fmt.Fprintf(os.Stderr, "unknown sync mode %q — use full, batched, or none\n", *syncMode)
		os.Exit(1)
	}

	// Open the database.
	db, err := vaultkv.Open(vaultkv.Options{
		Dir:          *dataDir,
		SyncMode:     sm,
		MemTableSize: int64(*memMB) * 1024 * 1024,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vaultkv: open: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Start TCP listener.
	addr := ":" + *port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer ln.Close()

	fmt.Printf("VaultKV listening on %s  data=%s  sync=%s  mem=%dMB\n",
		addr, *dataDir, *syncMode, *memMB)

	// Graceful shutdown on SIGINT / SIGTERM.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		ln.Close()
	}()

	// Accept loop — one goroutine per connection.
	for {
		conn, err := ln.Accept()
		if err != nil {
			break
		}
		go handleConn(conn, db)
	}

	fmt.Println("VaultKV stopped.")
}

// handleConn handles a single client connection.
//
// Protocol (line-oriented text, CRLF terminated):
//
//	SET <key> <value>\r\n  →  +OK\r\n
//	GET <key>\r\n          →  +<value>\r\n  |  -ERR not found\r\n
//	DEL <key>\r\n          →  +OK\r\n
//	PING\r\n               →  +PONG\r\n
//	STAT\r\n               →  +{json stats}\r\n
func handleConn(conn net.Conn, db *vaultkv.DB) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		switch cmd {

		case "SET":
			if len(parts) < 3 {
				fmt.Fprintf(conn, "-ERR SET requires key and value\r\n")
				continue
			}
			if err := db.Set([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Fprintf(conn, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(conn, "+OK\r\n")
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "-ERR GET requires key\r\n")
				continue
			}
			val, err := db.Get([]byte(parts[1]))
			if err != nil {
				fmt.Fprintf(conn, "-ERR %v\r\n", err)
			} else if val == nil {
				fmt.Fprintf(conn, "-ERR not found\r\n")
			} else {
				fmt.Fprintf(conn, "+%s\r\n", val)
			}

		case "DEL":
			if len(parts) < 2 {
				fmt.Fprintf(conn, "-ERR DEL requires key\r\n")
				continue
			}
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintf(conn, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(conn, "+OK\r\n")
			}

		case "PING":
			fmt.Fprintf(conn, "+PONG\r\n")

		case "STAT":
			s := db.Stats()
			fmt.Fprintf(conn,
				"+{\"memtable_bytes\":%d,\"memtable_entries\":%d,\"sstables\":%d,\"disk_bytes\":%d}\r\n",
				s.MemTableBytes, s.MemTableEntries, s.SSTables, s.TotalDiskBytes)

		default:
			fmt.Fprintf(conn, "-ERR unknown command %q\r\n", cmd)
		}
	}
}