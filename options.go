package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// Options holds variables from command line parsing.
type Options struct {
	MDBFiles      []string
	DatabaseURI   string
	Encoding      string
	Transaction   string
	TextEncoding  string
	BlobEncoding  string
	ListEncodings bool
	CheckTable    bool
	LogQuery      bool

	DSN     string
	Backend string
	TextEnc encoding.Encoding
	BlobEnc encoding.Encoding
}

// GetOptions parse command line arguments.
func GetOptions(name string, args []string) (opts Options, err error) {
	f := flag.NewFlagSet(name, flag.ContinueOnError)

	f.StringVar(&opts.Transaction, "transaction", "full", "full: single transaction for all mdbfiles. file: single transaction per mdbfiles. table: single transaction per table in mdbfiles.")
	f.StringVar(&opts.TextEncoding, "text-encoding", "", "use specified endocing to decode text type.")
	f.StringVar(&opts.BlobEncoding, "blob-encoding", "", "use specified endocing to decode blob type.")
	f.BoolVar(&opts.ListEncodings, "encodings", false, "list available encoding then exits.")
	f.BoolVar(&opts.CheckTable, "check", false, "add IF NOT EXISTS clause in CREAE TABLE")
	f.BoolVar(&opts.LogQuery, "log-query", false, "log executed query")
	f.Usage = func() {
		fmt.Fprintf(f.Output(), "usage: %s [options] <dburi> [mdbfiles ...]\n", name)
		fmt.Fprintln(f.Output())
		fmt.Fprintf(f.Output(), "options:\n")
		f.PrintDefaults()
		fmt.Fprintln(f.Output())
	}

	if err = f.Parse(args); err != nil {
		return
	}

	if f.NArg() < 2 {
		return opts, errors.New("dburi and at least one mdbfile are required.")
	}

	switch opts.Transaction {
	case "full", "file", "table":
	default:
		return opts, fmt.Errorf("unknown transaction mode %q", opts.Transaction)
	}

	opts.DatabaseURI = f.Arg(0)
	opts.MDBFiles = f.Args()[1:]

	opts.Backend, opts.DSN, err = uriToDSN(opts.DatabaseURI)
	if err != nil {
		return
	}

	if opts.ListEncodings {
		return opts, ListEncodings()
	}

	if opts.TextEncoding != "" {
		if opts.TextEnc, err = encodingByName(opts.TextEncoding); err != nil {
			return
		}
	}

	if opts.BlobEncoding != "" {
		if opts.BlobEnc, err = encodingByName(opts.BlobEncoding); err != nil {
			return
		}
	}

	return
}

// ListEncodings print list of available encodings to os.Stdout then
// returns flag.ErrHelp.
func ListEncodings() error {
	fmt.Println("These names are case insensitive:")
	for _, enc := range charmap.All {
		fmt.Printf("    %s\n", encodingName(enc))
	}
	return flag.ErrHelp
}

func encodingName(enc encoding.Encoding) string {
	name := fmt.Sprint(enc)
	return strings.ReplaceAll(name, " ", "-")
}

func encodingByName(name string) (encoding.Encoding, error) {
	for _, enc := range charmap.All {
		if strings.ToLower(name) == strings.ToLower(encodingName(enc)) {
			return enc, nil
		}
	}
	return nil, fmt.Errorf("encoding %q does not exist.", name)
}

func uriToDSN(dburi string) (backend string, dsn string, err error) {
	u, err := url.Parse(dburi)
	if err != nil {
		return backend, dsn, fmt.Errorf("invalid dburi %q", dburi)
	}
	if u.Scheme == "" {
		return backend, dsn, fmt.Errorf("invalid dburi %q", dburi)
	}

	switch u.Scheme {
	case "pg", "postgres", "postgresql":
		backend = "postgres"
		u.Scheme = "postgres"
		query := u.Query()
		query.Set("application_name", fmt.Sprintf("%s(%s)", ProgName, ProgVersion))
		u.RawQuery = query.Encode()
		dsn = u.String()
		return
	case "mysql", "mariadb":
		backend = "mysql"
		u.Scheme = ""
		dsn = u.String()
		if strings.HasPrefix(dsn, "//") {
			dsn = dsn[2:]
		}
		return
	case "sqlite", "sqlite3":
		backend = "sqlite3"
		switch {
		case len(u.Path) > 0 && u.Path[0] == '/':
			dsn = fmt.Sprintf("file:%s?%s", u.Path[1:], u.Query().Encode())
		case len(u.Path) > 0:
			dsn = fmt.Sprintf("file:%s?%s", u.Path, u.Query().Encode())
		default:
			return backend, dsn, errors.New("sqlite3 need to specify file name")
		}
		return
	default:
		return backend, dsn, fmt.Errorf("%q is not a supported database backend", u.Scheme)
	}
}
