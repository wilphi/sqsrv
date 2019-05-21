package client

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqtypes"
)

// Config -
type Config struct {
	enc         *gob.Encoder
	dec         *gob.Decoder
	conn        net.Conn
	isConnected bool
}

func init() {
	gob.Register(sqtypes.SQString{})
	gob.Register(sqtypes.SQInt{})
	gob.Register(sqtypes.SQBool{})
	gob.Register(sqtypes.SQNull{})
}

// SetConn - set the connection for the server to communicate on
func SetConn(conn net.Conn) *Config {
	return &Config{enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn), conn: conn, isConnected: true}
}

// Close -
func (clnt *Config) Close() (err error) {
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
func (clnt *Config) SendRequest(req sqprotocol.RequestToServer) error {

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
func (clnt *Config) ReceiveResponse() (*sqprotocol.ResponseToClient, error) {
	resp := &sqprotocol.ResponseToClient{}
	err := clnt.dec.Decode(resp)
	if err != nil {
		log.Println("Error receiving request reponse: ", err.Error())
		return nil, err
	}
	return resp, nil
}

// ReceiveColumns -
func (clnt *Config) ReceiveColumns(nCols int) ([]sqprotocol.ColInfo, error) {
	var cols []sqprotocol.ColInfo

	cols = make([]sqprotocol.ColInfo, nCols)
	for i := 0; i < nCols; i++ {
		cInfo := &sqprotocol.ColInfo{}
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
func (clnt *Config) ReceiveRow() (*sqprotocol.RowData, error) {
	rw := &sqprotocol.RowData{}
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
