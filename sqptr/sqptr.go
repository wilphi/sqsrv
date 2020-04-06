package sqptr

// SQPtr defines a single row pointer in SQ
type SQPtr uint64

// SQPtrs defines an array of SQPtr
type SQPtrs []SQPtr

// NotIn returns all items in A that are not in B
func NotIn(a, b SQPtrs) SQPtrs {
	var ret SQPtrs
	for _, x := range a {
		if !Contain(b, x) {
			ret = append(ret, x)
		}
	}
	return ret
}

// Contain returns true if the array of pointers contains the item pointer
func Contain(arr SQPtrs, item SQPtr) bool {
	for _, x := range arr {
		if x == item {
			return true
		}
	}
	return false
}
