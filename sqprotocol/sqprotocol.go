package sqprotocol

import "github.com/wilphi/sqsrv/sqtypes"

// RequestToServer -
type RequestToServer struct {
	Cmd string
}

// ResponseToClient -
type ResponseToClient struct {
	Msg         string
	IsErr       bool
	HasData     bool
	NRows       int
	NCols       int
	CMDResponse bool
}

//ColInfo -
type ColInfo struct {
	ColName string
	Width   int
	Align   int // 1 for right, -1 for left
}

//RowData -
type RowData struct {
	RowNum int
	Data   []sqtypes.Value
}
