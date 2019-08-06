package sqtest

import (
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

///////////////////////////////////////
//This package is for common test utilities used in SQSRV
//
///////////////////////////////////////

var doOnce sync.Once

func TestInit(logname string) {
	doOnce.Do(func() {
		// setup logging
		logFile, err := os.OpenFile(logname, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
	})
}
