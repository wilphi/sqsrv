package sqbin

import "github.com/wilphi/converse/sqbin"

// Encoder interface for objects that impliment the Encode function
type Encoder interface {
	Encode(*sqbin.Codec)
}

// Decoder interface for objects that implement the Decode function
type Decoder interface {
	Decode(*sqbin.Codec)
}
