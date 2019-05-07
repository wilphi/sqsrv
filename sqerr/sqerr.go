package sqerr

// SQError type
type SQError struct {
	msg     string // description of error
	errType string
}

// New - Create a new Error
func New(text string) error {
	return &SQError{text, "Error"}
}

// NewSyntax - Create a new Syntax Error
func NewSyntax(text string) error {
	return &SQError{text, "Syntax Error"}
}

// NewInternal - Create a new internal error
func NewInternal(text string) error {
	return &SQError{text, "Internal Error"}

}
func (e *SQError) Error() string { return e.errType + ": " + e.msg }
