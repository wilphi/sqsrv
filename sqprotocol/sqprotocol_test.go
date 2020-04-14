package sqprotocol_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

type Addr struct {
}

func (a *Addr) Network() string {
	return "Network"
}
func (a *Addr) String() string {
	return "Address value"
}

type connTester struct {
	buff     bytes.Buffer
	isClosed bool
	Err      error
}

func (c *connTester) Read(b []byte) (n int, err error) {
	if c.Err != nil {
		return 0, c.Err
	}
	if c.isClosed {
		return 0, errors.New("Connection is closed")
	}
	return c.buff.Read(b)
}

func (c *connTester) Write(b []byte) (n int, err error) {
	if c.Err != nil {
		return 0, c.Err
	}
	if c.isClosed {
		return 0, errors.New("Connection is closed")
	}
	return c.buff.Write(b)
}

func (c *connTester) Close() error {
	if c.isClosed == true {
		panic("The connTester connection is already closed")
	}
	c.isClosed = true
	return nil
}

func (c *connTester) LocalAddr() net.Addr {
	return &Addr{}
}

func (c *connTester) RemoteAddr() net.Addr {
	return &Addr{}
}

func (c *connTester) SetDeadline(t time.Time) error {
	return nil
}

func (c *connTester) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *connTester) SetWriteDeadline(t time.Time) error {
	return nil
}

func TestConnection(t *testing.T) {
	var myConn net.Conn
	var client *sqprotocol.ClientConfig
	var svr *sqprotocol.SvrConfig

	testConn := &connTester{}
	myConn = testConn
	t.Run("Set Client Conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		client = sqprotocol.SetClientConn(myConn)
		if client == nil {
			t.Error("Nil was returned")
			return
		}

	})
	t.Run("Set Server Conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		svr = sqprotocol.SetSvrConn(myConn, 1)
		if svr == nil {
			t.Error("Nil was returned")
			return
		}
		actConn := sqprotocol.ShowConn()
		if actConn != "Current Connections\n1\tAddress value\n" {
			t.Errorf("Connection was not added properly to connection list \n%s", actConn)
			return
		}
	})

	t.Run("Send Client Request", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		cReq := sqprotocol.RequestToServer{Cmd: "test command"}
		client.SendRequest(cReq)
		if client == nil {
			t.Error("Nil was returned")
			return
		}

	})

	t.Run("Send Client Request broken pipe", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("blah blah write: broken pipe blah blah")
		cReq := sqprotocol.RequestToServer{Cmd: "test command2"}
		err := client.SendRequest(cReq)
		ExpErr := "blah blah write: broken pipe blah blah"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
		}

	})
	t.Run("Send Client Request Other Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Other Error")
		cReq := sqprotocol.RequestToServer{Cmd: "test command2"}
		err := client.SendRequest(cReq)
		ExpErr := "Other Error"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
		}

	})

	t.Run("Server Receive", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		req, err := svr.ReceiveRequest()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}
		ExpMsg := "test command"
		if req.Cmd != ExpMsg {
			t.Errorf("Expected Msg: %s, Actual Msg: %s", ExpMsg, req.Cmd)
		}
	})

	t.Run("Server Receive EOF", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = io.EOF
		_, err := svr.ReceiveRequest()
		ExpErr := "EOF"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
		}

	})

	t.Run("Server Receive Other Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Other Error")
		_, err := svr.ReceiveRequest()
		ExpErr := "Other Error"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
		}

	})

	resp := sqprotocol.ResponseToClient{Msg: "Test", IsErr: true, HasData: true, NRows: 5, NCols: 10, CMDResponse: true}

	t.Run("Server Send Response", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		err := svr.SendResponse(&resp)
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})

	t.Run("Server Send Response with error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error send response")
		err := svr.SendResponse(&resp)
		ExpErr := "Error send response"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
		}

	})

	t.Run("Client Receive Response", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil

		svrResp, err := client.ReceiveResponse()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

		if !reflect.DeepEqual(resp, *svrResp) {
			t.Errorf("What the client sent %v was not what the server received %v", resp, svrResp)
			return
		}
	})
	t.Run("Client Receive Response Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error from Server")

		svrResp, err := client.ReceiveResponse()
		ExpErr := "Error from Server"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
			return
		}
		if svrResp != nil {
			t.Error("Unexpected response: Expecting nil but did not get it")
		}
	})

	t.Run("Server Send Columns", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		err := svr.SendColumns([]sqtables.ColDef{
			sqtables.NewColDef("col1", tokens.Int, false),
			sqtables.NewColDef("col2", tokens.String, true),
			sqtables.NewColDef("col3", tokens.Bool, false),
			sqtables.NewColDef("col4", tokens.Float, true),
		})
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})
	t.Run("Server Send Columns Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error from Server")
		err := svr.SendColumns([]sqtables.ColDef{sqtables.NewColDef("Col1", tokens.Int, false), sqtables.NewColDef("col2", tokens.String, true)})
		ExpErr := "Error from Server"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
			return
		}
	})

	t.Run("Client Receive Cols", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil

		colsResp, err := client.ReceiveColumns(4)
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}
		cols := []sqprotocol.ColInfo{
			{"col1", sqtypes.SQIntWidth},
			{"col2", -sqtypes.SQStringWidth},
			{"col3", sqtypes.SQBoolWidth},
			{"col4", sqtypes.SQFloatWidth},
		}
		if !reflect.DeepEqual(cols, colsResp) {
			t.Errorf("What the client sent %v was not what the server received %v", cols, colsResp)
			return
		}
	})

	t.Run("Client Receive Cols Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error from Server")
		colsResp, err := client.ReceiveColumns(4)
		ExpErr := "Error from Server"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}

		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
			return
		}
		if colsResp != nil {
			t.Error("Unexpected response: Expecting nil but did not get it")
		}
	})

	t.Run("Server Send Rows", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		for i := 1; i < 4; i++ {
			err := svr.SendRow(i, sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{i, "test", true, 2.1 * float64(i)}))
			if err != nil {
				t.Errorf("Unexpected Error: %s", err.Error())
				return
			}
		}

	})
	t.Run("Server Send Row Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error from Server")
		err := svr.SendRow(10, sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{10, "test", true, 20.1}))
		ExpErr := "Error from Server"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}
		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
			return
		}
	})

	t.Run("Client Receive Rows", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		for i := 1; i < 4; i++ {
			DataResp, err := client.ReceiveRow()
			if err != nil {
				t.Errorf("Unexpected Error: %s", err.Error())
				return
			}
			expData := sqprotocol.RowData{i, sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{i, "test", true, 2.1 * float64(i)})}
			if !reflect.DeepEqual(expData, *DataResp) {
				t.Errorf("What the server sent %v was not what the client received %v", DataResp, expData)
				return
			}
		}
	})
	t.Run("Client Receive Row Error", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = errors.New("Error from Server")
		DataResp, err := client.ReceiveRow()
		ExpErr := "Error from Server"
		if err == nil {
			t.Errorf("Unexpected Success, expecting error %s", ExpErr)
			return
		}

		if err.Error() != ExpErr {
			t.Errorf("Expecting Error: %s, Actual Err: %s", ExpErr, err.Error())
			return
		}
		if DataResp != nil {
			t.Error("Unexpected response: Expecting nil but did not get it")
		}
	})
	t.Run("Client Receive Row EOF", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = io.EOF
		DataResp, err := client.ReceiveRow()

		if err != nil {
			t.Errorf("Unexpected Error:  %s", err.Error())
			return
		}
		if DataResp != nil {
			t.Error("Unexpected response: Expecting nil but did not get it")
		}
	})

	t.Run("Client Close Conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil

		err := client.Close()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})
	t.Run("Client double Close Conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil

		err := client.Close()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})
	t.Run("Server Close Conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		testConn.isClosed = false

		err := svr.Close()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})

	t.Run("Server Close Invalid conn", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, true)

		testConn.Err = nil

		err := svr.Close()
		if err != nil {
			t.Errorf("Unexpected Error: %s", err.Error())
			return
		}

	})

	t.Run("Server Shutdown", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		testConn.Err = nil
		if sqprotocol.IsShutdown() {
			t.Errorf("IsShutdown should not be true before shutdown")
			return
		}
		// Setup 2 client connections
		svr1 := sqprotocol.SetSvrConn(&connTester{}, 1)
		svr2 := sqprotocol.SetSvrConn(&connTester{}, 2)
		go func() {
			for {
				if sqprotocol.IsShutdown() {
					svr1.Close()
					svr2.Close()
					break
				}
				time.Sleep(101 * time.Millisecond)
			}
		}()
		str := sqprotocol.ShowConn()
		fmt.Println(str)
		if str != "Current Connections\n1\tAddress value\n2\tAddress value\n" {
			t.Error("Connection list does not match expected")
			return
		}
		sqprotocol.Shutdown()

		if !sqprotocol.IsShutdown() {
			t.Errorf("IsShutdown should not be false after shutdown")
			return
		}
		str = sqprotocol.ShowConn()
		if str != "Current Connections\n" {
			t.Error("Not all connections have been terminated")
			return
		}

	})

	t.Run("Server Shutdown with timeout", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		sqprotocol.CancelShutdown()
		sqprotocol.ShutdownCount = 5
		testConn.Err = nil
		if sqprotocol.IsShutdown() {
			t.Errorf("IsShutdown should not be true before shutdown")
			return
		}
		// Setup 2 client connections
		_ = sqprotocol.SetSvrConn(&connTester{}, 1)
		_ = sqprotocol.SetSvrConn(&connTester{}, 2)

		str := sqprotocol.ShowConn()
		fmt.Println(str)
		if str != "Current Connections\n1\tAddress value\n2\tAddress value\n" {
			t.Error("Connection list does not match expected")
			return
		}
		sqprotocol.Shutdown()

		if !sqprotocol.IsShutdown() {
			t.Errorf("IsShutdown should not be false after shutdown")
			return
		}
		str = sqprotocol.ShowConn()
		if str != "Current Connections\n1\tAddress value\n2\tAddress value\n" {
			t.Error("Connection list does not match expected after shutdown")
			return
		}

	})

}
