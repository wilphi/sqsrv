package sqtest

import (
	"bufio"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sq"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/tokens"
)

///////////////////////////////////////
//This package is for common test utilities used in SQSRV
//
///////////////////////////////////////

var doOnce sync.Once

// TestInit initializes logging for tests
func TestInit(logname string) {
	doOnce.Do(func() {
		// setup logging
		logFile, err := os.OpenFile(logname, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		mode := os.Getenv("SQSRV_MODE")
		if mode == "DEBUG" {
			log.SetLevel(log.DebugLevel)
			log.SetOutput(os.Stdout)
		} else {
			log.SetOutput(logFile)

		}
	})
}

// ProcessSQFile loads sq commands from file
func ProcessSQFile(name string) error {
	profile := sqprofile.CreateSQProfile()
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	//	var lines []string
	scanner := bufio.NewScanner(file)
	//_ = scanner.Text()
	for scanner.Scan() {

		line := scanner.Text()
		tkns := tokens.Tokenize(line)
		sqfunc := sq.GetDispatchFunc(*tkns)
		_, _, err = sqfunc(profile, tkns)
		if err != nil {
			return err
		}
	}
	return nil
}
