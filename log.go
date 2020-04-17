package main

import (
	stdlog "log"
	"os"
)

var log = stdlog.New(os.Stdout, "", stdlog.Lshortfile)
