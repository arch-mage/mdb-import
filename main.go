package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	ctx := WithHandledSignal()
	err := run(ctx)

	if err == nil || err == context.Canceled || err == flag.ErrHelp {
		return
	}

	fmt.Fprintf(os.Stderr, "%#v\n", err)
	os.Exit(1)
}

func run(ctx context.Context) error {
	opts, err := GetOptions(os.Args[0], os.Args[1:])
	if err != nil {
		return err
	}

	log.Printf("establishing database connection using %q", opts.DatabaseURI)

	db, err := sql.Open(opts.Backend, opts.DSN)
	if err != nil {
		return err
	}
	defer db.Close()

	// sql.Open does not actually establish database connection. So,
	// check connection here.
	if err := db.Ping(); err != nil {
		return err
	}

	var tx *sql.Tx
	if opts.Transaction == "full" {
		log.Println("begin")
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			return err
		}
	}

	for _, file := range opts.MDBFiles {
		err := CopyDatabase(ctx, db, tx, opts, file)
		if err == nil {
			continue
		}
		if tx != nil {
			log.Println("rollback")
			tx.Rollback()
		}
		return err
	}

	if tx != nil {
		log.Println("commit")
		return tx.Commit()
	}

	return nil
}
