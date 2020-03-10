package main

import (
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/wilphi/sqsrv/sq"

	log "github.com/sirupsen/logrus"
)

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
	sq.Main()
}
