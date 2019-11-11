package sqprotocol

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

var connList []*SvrConfig
var inShutdown *int64
var mux sync.RWMutex

// ShutdownCount Controls how long a shutdown will wait for connections to terminate on their own
// once the time is up shutdown will continue
var ShutdownCount = 200

func init() {
	gob.Register(sqtypes.SQString{})
	gob.Register(sqtypes.SQInt{})
	gob.Register(sqtypes.SQBool{})
	gob.Register(sqtypes.SQNull{})
	gob.Register(sqtypes.SQFloat{})
	inShutdown = new(int64)
}

// SetSvrConn - set the connection for the server to communicate on
func SetSvrConn(conn net.Conn, cNum int) *SvrConfig {
	c := &SvrConfig{enc: gob.NewEncoder(conn), dec: gob.NewDecoder(conn), conn: conn, cNum: cNum}
	mux.Lock()
	defer mux.Unlock()
	connList = append(connList, c)
	return c
}

// ReceiveRequest -
func (srv *SvrConfig) ReceiveRequest() (*RequestToServer, error) {
	req := &RequestToServer{}
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
func (srv *SvrConfig) SendResponse(resp *ResponseToClient) error {
	err := srv.enc.Encode(resp)
	if err != nil {
		log.Errorln("Error Writing to client connection", err)
		return err
	}
	return nil
}

// Close -
func (srv *SvrConfig) Close() error {
	mux.Lock()
	defer mux.Unlock()
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
	err := srv.conn.Close()
	return err
}

// SendColumns -
func (srv *SvrConfig) SendColumns(cols []sqtables.ColDef) error {
	for _, c := range cols {
		cInfo := ColInfo{ColName: c.ColName, Width: getTypeWidth(c.ColType)}
		err := srv.enc.Encode(cInfo)
		if err != nil {
			log.Errorln("Error Writing to client connection", err)
			return err
		}

	}
	return nil
}

// SendRow -
func (srv *SvrConfig) SendRow(rowNum int, data []sqtypes.Value) error {
	rw := RowData{RowNum: rowNum, Data: data}
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
	case tokens.TypeInt:
		ret = sqtypes.SQIntWidth
	case tokens.TypeString:
		ret = -sqtypes.SQStringWidth
	case tokens.TypeBool:
		ret = sqtypes.SQBoolWidth
	case tokens.TypeFloat:
		ret = sqtypes.SQFloatWidth
	default:
		// This should never happen
		log.Panicf("Invalid type: %s", typeName)
	}
	return ret
}

//ShowConn builds a string that lists all connections to the server
func ShowConn() string {
	mux.Lock()
	defer mux.Unlock()
	str := "Current Connections\n"
	for _, c := range connList {
		// ConnNum, IP
		str += fmt.Sprintf("%d\t%v\n", c.cNum, c.conn.RemoteAddr())
	}
	return str
}

//Shutdown terminates connections in orderly fashion
func Shutdown() {
	if atomic.CompareAndSwapInt64(inShutdown, 0, 1) {

		cnt := 0
		for {
			// All connections a have terminated
			mux.Lock()
			numConn := len(connList)
			mux.Unlock()
			if numConn == 0 {
				break
			}

			// if more than 10 seconds has passed continue with shutdown
			if cnt > ShutdownCount {
				log.Info("Connection timeout for shutdown")
				break
			}
			// wait for a little bit
			time.Sleep(100 * time.Millisecond)
			cnt++
		}
	}
}

// IsShutdown is true if a shutdown is in process
func IsShutdown() bool {
	return atomic.LoadInt64(inShutdown) == 1
}

// CancelShutdown will stop an inprocess shutdown
func CancelShutdown() {
	atomic.StoreInt64(inShutdown, 0)
}
