package main

import (
	"flag"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sq"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqtables"
)

var host, port, tlog, dbfiles *string

func init() {
	// setup logging
	logFile, err := os.Create("sqsrv.log")
	if err != nil {
		panic(err)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	log.SetLevel(log.InfoLevel)

	//debug.SetGCPercent(400)
}

func profilerHTTP() {
	log.Info("Starting WebServer....")
	log.Infoln(http.ListenAndServe("localhost:6060", nil))
}
func main() {
	host = flag.String("host", "localhost", "Host name of the server")
	port = flag.String("port", "3333", "TCP port for server to listen on")
	tlog = flag.String("tlog", "./transaction.tlog", "File path/name for the transaction log")
	dbfiles = flag.String("dbfile", "./dbfiles/", "Directory where database files are stored")
	flag.Parse()

	// Set where datafile are
	sqtables.SetDBDir(*dbfiles)
	redo.SetTLog(*tlog)

	sq.SQMain(*host, *port)
}
