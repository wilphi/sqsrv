package sqerr

import "fmt"

// SQError type
type SQError struct {
	msg     string // description of error
	errType string
}

// New - Create a new Error
func New(text string) error {
	return &SQError{text, "Error"}
}

// Newf - Create a new Error with a formatted string
func Newf(format string, a ...interface{}) error {
	return &SQError{fmt.Sprintf(format, a...), "Error"}
}

// NewSyntax - Create a new Syntax Error
func NewSyntax(text string) error {
	return &SQError{text, "Syntax Error"}
}

// NewSyntaxf creates a new Syntax Error with the text formatted
func NewSyntaxf(format string, a ...interface{}) error {
	return &SQError{fmt.Sprintf(format, a...), "Syntax Error"}
}

// NewInternal - Create a new internal error
func NewInternal(text string) error {
	return &SQError{text, "Internal Error"}

}

// NewInternalf - Create a new Internal Error with a formatted string
func NewInternalf(format string, a ...interface{}) error {
	return &SQError{fmt.Sprintf(format, a...), "Internal Error"}
}

func (e *SQError) Error() string { return e.errType + ": " + e.msg }
