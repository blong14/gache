package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gache "github.com/blong14/gache/database"
)

func mustGetDB() *sql.DB {
	dsn, ok := os.LookupEnv("dsn")
	if !ok {
		dsn = gache.MEMORY
	}
	db, err := sql.Open("gache", dsn)
	if err != nil {
		panic(err)
	}
	if err = db.Ping(); err != nil {
		panic(err)
	}
	return db
}

func main() {
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go accept(ctx, mustGetDB())

	var s os.Signal
	select {
	case sig, ok := <-sigint:
		if ok {
			s = sig
		}
	}
	log.Printf("\nreceived %s signal\n", s)
	cancel()
	time.Sleep(500 * time.Millisecond)
}

func accept(ctx context.Context, db *sql.DB) {
	fmt.Print("\n% ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		start := time.Now()
		var result *gache.QueryResponse
		if err := db.QueryRowContext(ctx, scanner.Text()).Scan(&result); err != nil {
			log.Println(err)
			fmt.Print("\n% ")
			continue
		}
		fmt.Print("%\tkey\t\tvalue\n")
		if result.Success {
			fmt.Printf("1.\t%s\t\t%s\n", string(result.Key), result.Value)
		}
		fmt.Printf("[%s]", time.Since(start))
		fmt.Print("\n% ")
	}
}
