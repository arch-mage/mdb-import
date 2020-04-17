package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/arch-mage/mdb"
	"golang.org/x/text/encoding"
)

func CopyDatabase(ctx context.Context, db *sql.DB, tx *sql.Tx, opts Options, filename string) (err error) {
	if tx == nil && opts.Transaction == "file" {
		log.Printf("begin %s\n", filename)
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	tables, err := mdb.Tables(file)
	if err != nil {
		return err
	}

	for _, table := range tables {
		if table.Sys {
			continue
		}
		err := CopyTable(ctx, db, tx, opts, file, filename, table)
		if err == nil {
			continue
		}
		if tx != nil && opts.Transaction == "file" {
			log.Printf("rollback %s\n", filename)
			tx.Rollback()
		}
		return err
	}

	if tx != nil && opts.Transaction == "file" {
		log.Printf("commit %s\n", filename)
		return tx.Commit()
	}

	return nil
}

func CopyTable(ctx context.Context, db *sql.DB, tx *sql.Tx, opts Options, file io.ReadSeeker, filename string, table mdb.Table) (err error) {
	if tx == nil {
		if opts.Transaction != "table" {
			// should unreachable
			return errors.New("could not make transaction")
		}
		if tx, err = db.BeginTx(ctx, nil); err != nil {
			return
		}
		log.Printf("begin %s@%s\n", filename, table.Name)
	}

	// create table
	var buff strings.Builder
	fmt.Fprintf(&buff, "CREATE TABLE")
	if opts.CheckTable {
		fmt.Fprintf(&buff, " IF NOT EXISTS")
	}
	fmt.Fprintf(&buff, " %s ", quote(opts.Backend, table.Name))
	fmt.Fprintf(&buff, "(")
	if len(table.Columns) > 0 {
		fmt.Fprintf(
			&buff,
			"%s %s",
			quote(opts.Backend, table.Columns[0].Name),
			convertType(opts.Backend, table.Columns[0].Type),
		)
		for _, col := range table.Columns[1:] {
			fmt.Fprintf(
				&buff,
				", %s %s",
				quote(opts.Backend, col.Name),
				convertType(opts.Backend, col.Type),
			)
		}
	}
	fmt.Fprintf(&buff, ")")
	if opts.LogQuery {
		log.Println(buff.String())
	}

	if _, err := tx.ExecContext(ctx, buff.String()); err != nil {
		if opts.Transaction == "table" {
			log.Printf("rollback %s@%s\n", filename, table.Name)
			tx.Rollback()
		}
		return err
	}

	if err := CopyRows(ctx, tx, opts, file, table); err != nil {
		if opts.Transaction == "table" {
			log.Printf("rollback %s@%s\n", filename, table.Name)
			tx.Rollback()
		}
		return err
	}

	if opts.Transaction == "table" {
		log.Printf("commit %s@%s\n", filename, table.Name)
		return tx.Commit()
	}
	return
}

func CopyRows(ctx context.Context, tx *sql.Tx, opts Options, file io.ReadSeeker, table mdb.Table) error {
	var buff strings.Builder
	fmt.Fprintf(&buff, "INSERT INTO %s ", quote(opts.Backend, table.Name))
	fmt.Fprintf(&buff, "(")
	if len(table.Columns) > 0 {
		fmt.Fprintf(&buff, "%s", quote(opts.Backend, table.Columns[0].Name))
		for _, col := range table.Columns[1:] {
			fmt.Fprintf(&buff, ", %s", quote(opts.Backend, col.Name))
		}
	}
	fmt.Fprintf(&buff, ")")
	fmt.Fprintf(&buff, " VALUES ")
	fmt.Fprintf(&buff, "(")
	if len(table.Columns) > 0 {
		fmt.Fprintf(&buff, "%s", binding(opts.Backend, 1))
		for i := range table.Columns[1:] {
			fmt.Fprintf(&buff, ", %s", binding(opts.Backend, i+2))
		}
	}
	fmt.Fprintf(&buff, ")")
	if opts.LogQuery {
		log.Println(buff.String())
	}

	stmt, err := tx.Prepare(buff.String())
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := mdb.Rows(file, table.Name)
	if err != nil {
		return err
	}

	var textDec, blobDec *encoding.Decoder

	if opts.TextEnc != nil {
		textDec = opts.TextEnc.NewDecoder()
	}

	if opts.BlobEnc != nil {
		blobDec = opts.BlobEnc.NewDecoder()
	}

	for {
		fields, err := rows.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// apply encoding
		if textDec != nil || blobDec != nil {
			for i, col := range table.Columns {
				if fields[i] == nil {
					continue
				}

				// assumes mdb gives correct type
				switch col.Type {
				case "Text", "LongText":
					if textDec == nil {
						continue
					}
					textDec.Reset()
					fields[i], err = textDec.String(fields[i].(string))
				case "Binary", "LongBinary":
					if blobDec == nil {
						continue
					}
					blobDec.Reset()
					fields[i], err = textDec.Bytes(fields[i].([]byte))
				default:
					continue
				}
				if err != nil {
					return err
				}
			}
		}

		if _, err := stmt.ExecContext(ctx, fields...); err != nil {
			return err
		}
	}
}

func quote(backend, name string) string {
	switch backend {
	case "mysql":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

func convertType(backend, name string) string {
	return typeMap[backend][name]
}

func binding(backend string, num int) string {
	switch backend {
	case "postgres":
		return fmt.Sprintf("$%d", num)
	default:
		return "?"
	}
}

var typeMap = map[string]map[string]string{
	"postgres": map[string]string{
		"Bool":       "BOOL",
		"Byte":       "SMALLINT",
		"Int":        "INTEGER",
		"LongInt":    "INTEGER",
		"Money":      "MONEY",
		"Float":      "REAL",
		"Double":     "DOUBLE",
		"DateTime":   "TIMESTAMP",
		"Binary":     "BYTEA",
		"Text":       "TEXT",
		"LongBinary": "BYTEA",
		"LongText":   "TEXT",
		"GUID":       "UUID",
		"Numeric":    "NUMERIC",
	},
	"sqlite3": map[string]string{
		"Bool":       "BOOL",
		"Byte":       "BYTE",
		"Int":        "INTEGER",
		"LongInt":    "INTEGER",
		"Money":      "NUMERIC",
		"Float":      "REAL",
		"Double":     "REAL",
		"DateTime":   "DATETIME",
		"Binary":     "BLOB",
		"Text":       "TEXT",
		"LongBinary": "BLOB",
		"LongText":   "TEXT",
		"GUID":       "TEXT",
		"Numeric":    "NUMERIC",
	},
	"mysql": map[string]string{
		"Bool":       "BOOL",
		"Byte":       "TINYINT",
		"Int":        "INTEGER",
		"LongInt":    "INTEGER",
		"Money":      "NUMERIC",
		"Float":      "FLOAT",
		"Double":     "DOUBLE",
		"DateTime":   "DATETIME",
		"Binary":     "BLOB",
		"Text":       "TEXT",
		"LongBinary": "BLOB",
		"LongText":   "TEXT",
		"GUID":       "TEXT",
		"Numeric":    "NUMERIC",
	},
}
