package sqbin

// Encoder interface for objects that impliment the Encode function
type Encoder interface {
	Encode(*Codec)
}

// Decoder interface for objects that implement the Decode function
type Decoder interface {
	Decode(*Codec)
}
