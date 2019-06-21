package sqprotocol

import (
	"encoding/gob"
	"net"

	"github.com/wilphi/sqsrv/sqtypes"
)

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
}

//RowData -
type RowData struct {
	RowNum int
	Data   []sqtypes.Value
}

// SvrConfig -
type SvrConfig struct {
	enc         *gob.Encoder
	dec         *gob.Decoder
	conn        net.Conn
	cNum        int //connection number
	isConnected bool
}

// ClientConfig -
type ClientConfig struct {
	enc         *gob.Encoder
	dec         *gob.Decoder
	conn        net.Conn
	isConnected bool
}
