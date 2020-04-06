package sqprotocol

import (
	"encoding/gob"
	"fmt"
	"io"
	log "github.com/sirupsen/logrus"
	"net"
	"strings"

	"github.com/wilphi/sqsrv/sqtypes"
)

func init() {
	gob.Register(sqtypes.SQString{})
	gob.Register(sqtypes.SQInt{})
	gob.Register(sqtypes.SQBool{})
	gob.Register(sqtypes.SQNull{})
	gob.Register(sqtypes.SQFloat{})
}

// SetClientConn - set the connection for the server to communicate on
func SetClientConn(conn net.Conn) *ClientConfig {
	return &ClientConfig{enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn), conn: conn, isConnected: true}
}

// Close -
func (clnt *ClientConfig) Close() (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = nil
		}
	}()
	clnt.isConnected = false
	return clnt.conn.Close()
}

// SendRequest -
func (clnt *ClientConfig) SendRequest(req RequestToServer) error {

	err := clnt.enc.Encode(req)
	if err != nil {
		if strings.Contains(err.Error(), "write: broken pipe") {
			fmt.Println("Connection to Server has been lost, please try to reconnect")
			clnt.isConnected = false
			return err
		}
		log.Println("Encode Error:", err)
		return err
	}
	return nil
}

// ReceiveResponse -
func (clnt *ClientConfig) ReceiveResponse() (*ResponseToClient, error) {
	resp := &ResponseToClient{}
	err := clnt.dec.Decode(resp)
	if err != nil {
		//log.Println("Error receiving request reponse: ", err.Error())
		return nil, err
	}
	return resp, nil
}

// ReceiveColumns -
func (clnt *ClientConfig) ReceiveColumns(nCols int) ([]ColInfo, error) {
	var cols []ColInfo

	cols = make([]ColInfo, nCols)
	for i := 0; i < nCols; i++ {
		cInfo := &ColInfo{}
		err := clnt.dec.Decode(cInfo)
		if err != nil {
			log.Println("Error reading Column Info from server: ", err)
			return nil, err
		}
		cols[i] = *cInfo
	}
	return cols, nil
}

// ReceiveRow -
func (clnt *ClientConfig) ReceiveRow() (*RowData, error) {
	rw := &RowData{}
	err := clnt.dec.Decode(rw)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		log.Println("Error Reading rows from server: ", err)
		return nil, err
	}
	return rw, nil
}
