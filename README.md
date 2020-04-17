# mdb-import

> import mdb data to modern database

This is a command line program to import data from mdb JET3 database to
postgres, sqlite3 or mysql.

## usage

From `./mdb-import -help`:

    usage: ./mdb-import [options] <dburi> [mdbfiles ...]

    options:
      -blob-encoding string
            use specified endocing to decode blob type.
      -check
            add IF NOT EXISTS clause in CREAE TABLE
      -encodings
            list available encoding then exits.
      -log-query
            log executed query
      -text-encoding string
            use specified endocing to decode text type.
      -transaction string (default "full")
            full: single transaction for all mdbfiles.
            file: single transaction per mdbfiles.
            table: single transaction per table in mdbfiles.

