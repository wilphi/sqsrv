package sq

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqprotocol"
	protocol "github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/tokens"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
)

// SQVersion  - version of software
const SQVersion = "SQSRV v0.7.0"

const (
	cHost = "localhost"
	cPort = "3333"
	cType = "tcp"
)

// Main is the main process function for the SQServer
func Main(host, port string) {
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
	go listenerThread(host, port, Terminate)

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
		log.Info("Stop Accepting new connections")
		log.Info("Terminating all connections")
		sqprotocol.Shutdown()
		// wait for a little bit
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			fmt.Print(".")
		}
		fmt.Println("")

		log.Info("Flushing Redo")

		redo.Stop()
		log.Info("Checkpoint")
		cmdCheckpoint(profile, nil)
		return
	}

}

func listenerThread(host, port string, Terminate chan bool) {

	// Setup Listener
	l, err := net.Listen(cType, host+":"+port)
	if err != nil {
		log.Fatalln("Error Listening: (", host, ":", port, ") -- ", err.Error())
	}
	// Close listener when app closes
	defer l.Close()

	log.Println("Listening on ", host, ":", port)
	log.Println(SQVersion)

	for {
		if sqprotocol.IsShutdown() {
			break
		}

		//Listen for connections
		conn, err := l.Accept()

		if sqprotocol.IsShutdown() {
			break
		}

		if err != nil {
			log.Fatalln("Error accepting connection: ", err.Error())
		}

		profile := sqprofile.CreateSQProfile()
		srv := sqprotocol.SetSvrConn(conn, int(profile.GetID()))
		var wg *sync.WaitGroup
		wg = &sync.WaitGroup{}

		go processConnectionFunc(profile, srv, wg, Terminate)
	}
	log.Info("Listening for new connections terminated")
}

func processConnectionFunc(profile *sqprofile.SQProfile, srv *sqprotocol.SvrConfig, wg *sync.WaitGroup, Terminate chan bool) {

	defer srv.Close()

	log.Infoln("Processing Connection #", profile.GetID())

	for {
		wg.Wait()
		if sqprotocol.IsShutdown() {
			return
		}
		profile.VerifyNoLocks()
		req, err := srv.ReceiveRequest()
		if err != nil {
			return
		}
		log.Debugln(req.Cmd)
		wg.Wait()
		tkList := tokens.Tokenize(req.Cmd)

		resp := protocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: false}
		var data *sqtables.DataSet
		var isShutdown ShutdownType
		isShutdown = NoAction
		if tkList.Len() > 0 && tkList.Peek().GetName() == tokens.Ident {
			cmdFunc := GetCmdFunc(*tkList)
			if cmdFunc != nil {
				resp, isShutdown, err = cmdFunc(profile, tkList)
				if err != nil {
					log.Info(err)
					resp.IsErr = true
					resp.Msg = err.Error()
				}

				switch isShutdown {
				case Shutdown:
					srv.SendResponse(&resp)
					Terminate <- true
					return
				case ShutdownForce:
					log.Warn("Forced Shutdown initiated...")
					os.Exit(0)
				}
			} else {
				err = sqerr.New("Invalid Server command")
				log.Infoln(err)
				resp.IsErr = true
				resp.Msg = err.Error()

			}
		} else {
			dispFunc := GetDispatchFunc(*tkList)
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
					resp.NRows = data.Len()
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
			log.Info(fmt.Sprintf("%d rows written to client", data.Len()))

		}
	}
}
