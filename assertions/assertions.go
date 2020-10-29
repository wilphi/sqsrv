package assertions

import (
	"log"
	"strings"
)

// Very basic assertion package for invariant conditions within code
// uses logrus for logging

// Assert will panic with the given message when false
func Assert(condition bool, msg string) {
	if !condition {
		log.Panic(msg)
	}
}

//AssertNoErr will panic on error, an optional prefex for the message can be added
func AssertNoErr(err error, msg ...string) {
	if err != nil {
		str := strings.Join(msg, ": ") + err.Error()
		log.Panic(str)
	}
}
