package server

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	t "github.com/wilphi/sqsrv/tokens"
)

// Config -
type Config struct {
	enc  *gob.Encoder
	dec  *gob.Decoder
	conn net.Conn
	cNum int //connection number
}

var connList []*Config
var inShutdown bool

func init() {
	gob.Register(sqtypes.SQString{})
	gob.Register(sqtypes.SQInt{})
	gob.Register(sqtypes.SQBool{})
	gob.Register(sqtypes.SQNull{})
}

// SetConn - set the connection for the server to communicate on
func SetConn(conn net.Conn, cNum int) *Config {
	c := &Config{enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn), conn: conn, cNum: cNum}
	connList = append(connList, c)
	return c
}

// ReceiveRequest -
func (srv *Config) ReceiveRequest() (*sqprotocol.RequestToServer, error) {
	req := &sqprotocol.RequestToServer{}
	err := srv.dec.Decode(req)
	if err != nil {
		if err != io.EOF {
			log.Errorf("Error Reading request from Client Connection: \"%s\"\n", err.Error())
		}
		return nil, err
	}
	return req, nil
}

// SendResponse -
func (srv *Config) SendResponse(resp *sqprotocol.ResponseToClient) error {
	err := srv.enc.Encode(resp)
	if err != nil {
		log.Errorln("Error Writing to client connection", err)
		return err
	}
	return nil
}

// Close -
func (srv *Config) Close() error {
	idx := -1
	for i, c := range connList {
		if c.cNum == srv.cNum {
			// Remove from list
			idx = i
		}
	}
	if idx == -1 {
		log.Panic("Unable to find connection in connList")
	}
	connList = append(connList[:idx], connList[idx+1:]...)
	log.Infof("Closing Client Connection #%d\n", srv.cNum)

	return srv.conn.Close()
}

// SendColumns -
func (srv *Config) SendColumns(cols []sqtables.ColDef) error {
	for _, c := range cols {
		cInfo := sqprotocol.ColInfo{ColName: c.ColName, Width: getTypeWidth(c.ColType)}
		err := srv.enc.Encode(cInfo)
		if err != nil {
			log.Errorln("Error Writing to client connection", err)
			return err
		}

	}
	return nil
}

// SendRow -
func (srv *Config) SendRow(rowNum int, data []sqtypes.Value) error {
	rw := sqprotocol.RowData{RowNum: rowNum, Data: data}
	err := srv.enc.Encode(rw)
	if err != nil {
		log.Errorln("Error Writing to client connection", err)
		return err
	}
	return nil
}

func getTypeWidth(typeName string) int {
	var ret int
	switch typeName {
	case t.TypeInt:
		ret = sqtypes.SQIntWidth
	case t.TypeString:
		ret = -sqtypes.SQStringWidth
	case t.TypeBool:
		ret = sqtypes.SQBoolWidth
	default:
		ret = 0
	}
	return ret
}

//ShowConn builds a string that lists all connections to the server
func ShowConn() string {
	str := "Current Connections\n"
	for _, c := range connList {
		// ConnNum, IP
		str += fmt.Sprintf("%d\t%v\n", c.cNum, c.conn.RemoteAddr())
	}
	return str
}

//Shutdown terminates connections in orderly fashion
func Shutdown() {
	inShutdown = true

	cnt := 0
	for {
		// All connections a have terminated
		if len(connList) == 0 {
			break
		}

		// if more than 10 seconds has passed continue with shutdown
		if cnt > 200 {
			log.Info("Connection timeout for shutdown")
			break
		}
		// wait for a little bit
		time.Sleep(100 * time.Millisecond)
		cnt++
	}
}

// IsShutdown is true if a shutdown is in process
func IsShutdown() bool {
	return inShutdown
}
