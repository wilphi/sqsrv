package transid

import "sync/atomic"

var transID = new(uint64)

// GetTransID returns the last transaction id in database
func GetTransID() uint64 {
	return atomic.LoadUint64(transID)
}

//SetTransID sets the transaction id to the supplied number
func SetTransID(id uint64) {
	atomic.StoreUint64(transID, id)
}

//GetNextID -
func GetNextID() uint64 {
	return atomic.AddUint64(transID, 1)
}
