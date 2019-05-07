package sqerr_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/wilphi/sqsrv/sqerr"
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("sqerr_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}
func testErrsFunc(err error, expected string) func(*testing.T) {
	return func(t *testing.T) {
		if err.Error() != expected {
			t.Error(fmt.Sprintf("Expecting Error %s but got: %s", expected, err.Error()))
		}
	}
}
func TestSQErrs(t *testing.T) {

	t.Run("Error type test", testErrsFunc(sqerr.New("Test Error"), "Error: Test Error"))
	t.Run("Error type test", testErrsFunc(sqerr.NewSyntax("Test Error"), "Syntax Error: Test Error"))
	t.Run("Error type test", testErrsFunc(sqerr.NewInternal("Test Error"), "Internal Error: Test Error"))

}
