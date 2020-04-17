package sqtest

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

///////////////////////////////////////
//This package is for common test utilities used in SQSRV
//
///////////////////////////////////////

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	Errorf(format string, args ...interface{})
	Helper()
	Name() string
}

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

//CheckErr checks an error to see if it is expected/unexpected
// Returns:
//  true if an unsuccessful error check has occurred
//  false if the testing should continue
func CheckErr(t TestingT, err error, ExpErr string) bool {
	t.Helper()

	if err != nil {
		if ExpErr == "" {
			t.Errorf("Unexpected Error: %q", err.Error())
			return true
		}
		if ExpErr != err.Error() {
			t.Errorf("Expecting Error %q but got: %q", ExpErr, err.Error())
			return true
		}
		return true
	}
	if ExpErr != "" { // && err==nil
		t.Errorf("Unexpected Success should have returned error: %q", ExpErr)
		return true
	}
	return false
}

// PanicTrace Prints a simplified stack trace after a panic
func PanicTrace() (ret string) {
	skipFragment := []string{"sqtest/sqtest.go", "testing/testing.go", "runtime/", "/sirupsen/logrus/"}

	ret += fmt.Sprintln("==|Stack Trace|================")
	defer func() {
		ret += fmt.Sprintln("===============================")
	}()

	// Ask runtime.Callers for up to 10 pcs, including runtime.Callers itself.
	pc := make([]uintptr, 20)
	n := runtime.Callers(0, pc)
	if n == 0 {
		// No pcs available. Stop now.
		// This can happen if the first argument to runtime.Callers is large.
		return
	}

	pc = pc[:n] // pass only valid pcs to runtime.CallersFrames
	frames := runtime.CallersFrames(pc)
	panicFound := false
	// Loop to get frames.
	// A fixed number of pcs can expand to an indefinite number of Frames.
	indent := ""
outer:
	for {
		frame, more := frames.Next()
		// To keep this example's output stable
		// even if there are changes in the testing package,
		// stop unwinding when we leave package runtime.
		if strings.Contains(frame.File, "runtime/panic") {
			panicFound = true
			continue
		}
		if !panicFound {
			continue
		}
		for _, frag := range skipFragment {
			if strings.Contains(frame.File, frag) {
				continue outer
			}
		}
		//if strings.Contains(frame.File, "runtime/") || strings.Contains(frame.File, "") {
		//		continue
		//	}
		if frame.File != "" {
			ret += fmt.Sprintf("%s---> %s:%d - func %s\n", indent, frame.File, frame.Line, getFuncName(frame.Function))
			indent += "  "
		}
		if !more {
			break
		}
	}
	return
}

func getFuncName(a string) string {
	last := strings.LastIndex(a, "/")

	list := strings.Split(a[last+1:], ".")
	switch len(list) {
	case 0:
		return ""
	case 1:
		return list[0]
	case 2:
		return list[1]
	default:
		if strings.Contains(list[1], "(") {
			return list[1] + list[2]
		}
		return list[1]
	}

}

// PanicTestRecovery -
func PanicTestRecovery(t TestingT, expPanic bool) {
	var str string

	t.Helper()
	r := recover()

	if expPanic && r == nil {
		t.Errorf("%s did not panic", t.Name())
	}
	if !expPanic && r != nil {
		switch x := r.(type) {
		case string:
			str = x
		case error:
			str = x.Error()
		case *logrus.Entry:
			str = x.Message
		default:
			// Fallback err (per specs, error strings should be lowercase w/o punctuation
			str = fmt.Sprintf("Unknown panic: %T||%v", x, x)
		}

		t.Errorf("%s panicked unexpectedly: %s\n%s", t.Name(), str, PanicTrace())

	}
}
