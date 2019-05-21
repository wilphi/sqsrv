package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/serv"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"

	log "github.com/sirupsen/logrus"
	protocol "github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqprotocol/server"
	"github.com/wilphi/sqsrv/sqtables"
	t "github.com/wilphi/sqsrv/tokens"
)

// SQVersion  - version of software
const SQVersion = "SQSRV v0.6.15"

const (
	cHost = "localhost"
	cPort = "3333"
	cType = "tcp"
)

var host, port *string

const (
	cNormal        = 0
	cShutdown      = 1
	cShutdownForce = 2
)

// Job -
type Job struct {
	txID int
	conn net.Conn
}

func init() {
	// setup logging
	logFile, err := os.Create("sqsrv.log")
	if err != nil {
		panic(err)
	}

	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	log.SetLevel(log.InfoLevel)

	//debug.SetGCPercent(400)
}

func profilerHTTP() {
	log.Info("Starting WebServer....")
	log.Infoln(http.ListenAndServe("localhost:6060", nil))
}
func main() {
	host = flag.String("host", "localhost", "Host name of the server")
	port = flag.String("port", "3333", "TCP port for server to listen on")
	flag.Parse()

	//	var jobs = make(chan Job, 10)
	var Terminate = make(chan bool, 10)
	var doShutdown = false
	//go profilerHTTP()

	profile := sqprofile.CreateSQProfile()
	// Load the database
	err := sqtables.ReadDB(profile)
	if err != nil {
		log.Panic("Unable to load database: ", err)
	}

	if err := redo.Recovery(profile); err != nil {
		log.Fatal(err)
	}

	// startup RedoLog
	redo.Start()

	//go NewMonitor(10)
	// startup listener thread
	go listenerThread(Terminate)

	for !doShutdown {
		log.Debug("+++++++++++++++++ Main waiting for shutdown signal +++++++++++++++")
		doShutdown = <-Terminate
		if doShutdown {
			log.Info("Shutdown received")
			break
		}
	}
	if doShutdown {
		log.Info("Shutting down Server")
		return
	}

}

func listenerThread(Terminate chan bool) {

	// Setup Listener
	l, err := net.Listen(cType, *host+":"+*port)
	if err != nil {
		log.Fatalln("Error Listening: (", *host, ":", *port, ") -- ", err.Error())
	}
	// Close listener when app closes
	defer l.Close()

	log.Println("Listening on ", *host, ":", *port)
	log.Println(SQVersion)

	for {
		//Listen for connections
		conn, err := l.Accept()
		if err != nil {
			log.Fatalln("Error accepting connection: ", err.Error())
		}

		profile := sqprofile.CreateSQProfile()
		srv := server.SetConn(conn, int(profile.GetID()))
		var wg *sync.WaitGroup
		wg = &sync.WaitGroup{}

		go processConnectionFunc(profile, srv, wg, Terminate)
	}
}

func processConnectionFunc(profile *sqprofile.SQProfile, srv *server.Config, wg *sync.WaitGroup, Terminate chan bool) {

	defer srv.Close()

	log.Infoln("Processing Connection #", profile.GetID())

	for {
		wg.Wait()
		profile.VerifyNoLocks()
		req, err := srv.ReceiveRequest()
		if err != nil {
			return
		}
		log.Debugln(req.Cmd)
		wg.Wait()
		tkList := t.Tokenize(req.Cmd)

		resp := protocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: false}
		var data *sqtables.DataSet
		isShutdown := false
		if tkList.Len() > 0 && tkList.Peek().GetName() == t.Ident {
			cmdFunc := serv.GetCmdFunc(*tkList)
			if cmdFunc != nil {
				resp, isShutdown, err = cmdFunc(profile, tkList)
				if err != nil {
					log.Info(err)
					resp.IsErr = true
					resp.Msg = err.Error()
				}

				if isShutdown {
					srv.SendResponse(&resp)
					Terminate <- true
					return
				}
			} else {
				err = sqerr.New("Invalid Server command")
				log.Infoln(err)
				resp.IsErr = true
				resp.Msg = err.Error()

			}
		} else {
			dispFunc := serv.GetDispatchFunc(*tkList)
			if dispFunc != nil {
				if tkList.Peek().GetValue() == "checkpoint" {
					wg.Add(1)
					resp.Msg, data, err = dispFunc(profile, tkList)
					time.Sleep(10 * time.Second)
					wg.Done()
				} else {
					resp.Msg, data, err = dispFunc(profile, tkList)
				}
				if err != nil {
					log.Infoln(err)
					resp.IsErr = true
					resp.Msg = err.Error()
				}
				if data != nil {
					resp.HasData = true
					resp.NRows = data.NumRows()
					resp.NCols = data.NumCols()
				}
			} else {
				err = sqerr.New("Unable to dispatch command")
				log.Infoln(err)
				resp.IsErr = true
				resp.Msg = err.Error()

			}
		}

		err = srv.SendResponse(&resp)
		if err != nil {
			return
		}

		if resp.HasData {
			// There are rows to return
			//First the columns
			cl := data.GetColList()
			err = srv.SendColumns(cl.GetColDefs())
			if err != nil {
				return
			}
			for i := range data.Vals {
				err = srv.SendRow(i, data.Vals[i])
				if err != nil {
					return
				}

			}
			log.Info(fmt.Sprintf("%d rows written to client", data.NumRows()))

		}
	}
}
