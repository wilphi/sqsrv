package sqtest

import (
	"bufio"
	"fmt"
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

//CheckErr checks an error to see if it is expected/unexpected
// Returns:
//  msg - "" if the exepected result, otherwise a more detailed message
//  contEx - true if execution should continue, otherwise false
func CheckErr(err error, ExpErr string) (msg string, contEx bool) {
	if err != nil {
		if ExpErr == "" {
			return fmt.Sprintf("Unexpected Error: %s", err.Error()), false

		}
		if ExpErr != err.Error() {
			return fmt.Sprintf("Expecting Error %s but got: %s", ExpErr, err.Error()), false

		}
		return "", false
	}
	if ExpErr != "" { // && err==nil
		return fmt.Sprintf("Unexpected Success should have returned error: %s", ExpErr), false
	}
	return "", true
}
