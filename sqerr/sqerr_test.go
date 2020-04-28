package sqerr_test

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

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
			t.Errorf("Expecting Error %s but got: %s", expected, err.Error())
		}
	}
}
func TestSQErrs(t *testing.T) {

	t.Run("Error type test", testErrsFunc(sqerr.New("Test Error"), "Error: Test Error"))
	t.Run("Error type test with formatting", testErrsFunc(sqerr.Newf("Test %d Error %s", 2, "formatted"), "Error: Test 2 Error formatted"))
	t.Run("Syntax Error type test", testErrsFunc(sqerr.NewSyntax("Test Error"), "Syntax Error: Test Error"))
	t.Run("Syntax Error type test with formatting", testErrsFunc(sqerr.NewSyntaxf("Test %d Error %s", 4, "formatted-2"), "Syntax Error: Test 4 Error formatted-2"))
	t.Run("Internal Error type test", testErrsFunc(sqerr.NewInternal("Test Error"), "Internal Error: Test Error"))
	t.Run("Internal Error type test with formatting", testErrsFunc(sqerr.NewInternalf("Test %d Error %s", 6, "formatted-3"), "Internal Error: Test 6 Error formatted-3"))

}
