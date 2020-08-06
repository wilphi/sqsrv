package sq

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/tokens"

	"github.com/wilphi/sqsrv/redo"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
)

// SQVersion  - version of software
const SQVersion = "SQSRV v0.13"

//Options are the values set by the flag package
var Options struct {
	host       string
	port       string
	tlog       string
	dbfiles    string
	cpuprofile string
	lazytlog   int
}

// Main is the main process function for the SQServer
func Main() {

	//	var jobs = make(chan Job, 10)
	var Terminate = make(chan bool, 10)
	var doShutdown = false
	//go profilerHTTP()

	flag.StringVar(&Options.host, "host", "localhost", "Host name of the server")
	flag.StringVar(&Options.port, "port", "3333", "TCP port for server to listen on")
	flag.StringVar(&Options.tlog, "tlog", "./transaction.tlog", "File path/name for the transaction log")
	flag.StringVar(&Options.dbfiles, "dbfile", "./dbfiles/", "Directory where database files are stored")
	flag.StringVar(&Options.cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.IntVar(&Options.lazytlog, "lazytlog", 1000, "Number of milliseconds between file.Sync of the tlog. A value of 0 will sync after every write. Non zero values may lead to n milliseconds of dataloss")
	flag.Parse()

	if Options.cpuprofile != "" {
		f, err := os.Create(Options.cpuprofile)
		if err != nil {
			log.Panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	// Set where datafile are
	sqtables.SetDBDir(Options.dbfiles)
	redo.SetTLog(Options.tlog)

	log.Println("Starting SQSrv version", SQVersion)
	log.Println("dbfiles =", Options.dbfiles)
	log.Println("tlog =", Options.tlog)
	log.Println("host =", Options.host)
	log.Println("port =", Options.port)
	if Options.lazytlog != 0 {
		log.Printf("Lazy writting interval = %d Milliseconds", Options.lazytlog)
	} else {
		log.Println("Transactions are durably written to transaction log")
	}

	profile := sqprofile.CreateSQProfile()

	// Load the database
	err := sqtables.ReadDB(profile)
	if err != nil {
		log.Panic("Unable to load database: ", err)
	}

	redo.SetLazyTlog(Options.lazytlog)
	if err := redo.Recovery(profile); err != nil {
		log.Fatal(err)
	}

	// startup RedoLog
	redo.Start()

	//go NewMonitor(10)
	// startup listener thread
	go listenerThread(Options.host, Options.port, Terminate)

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

	log.Println("Starting Listener...")
	// Setup Listener
	l, err := net.Listen("tcp", host+":"+port)
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
		//profile.VerifyNoLocks()
		req, err := srv.ReceiveRequest()
		if err != nil {
			return
		}
		log.Debugln(req.Cmd)
		wg.Wait()
		tkList := tokens.Tokenize(req.Cmd)

		resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: false}
		var data *sqtables.DataSet
		var isShutdown ShutdownType
		isShutdown = NoAction
		if tkList.Len() > 0 && tkList.Peek().ID() == tokens.Ident {
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
			trans := sqtables.BeginTrans(profile, true)
			dispFunc := GetDispatchFunc(*tkList)
			if dispFunc != nil {
				vtkn, ok := tkList.Peek().(*tokens.ValueToken)
				if ok && vtkn.Value() == "checkpoint" {
					wg.Add(1)
					resp.Msg, data, err = dispFunc(trans, tkList)
					time.Sleep(10 * time.Second)
					wg.Done()
				} else {
					resp.Msg, data, err = dispFunc(trans, tkList)
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

			if resp.IsErr {
				trans.Rollback()
			} else {
				err = trans.Commit()
				if err != nil {
					trans.Rollback()
					resp.IsErr = true
					resp.Msg = "Error Committing transaction: " + err.Error()
				}
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
			err = srv.SendColumns(cl.GetRefs())
			if err != nil {
				return
			}
			for i := range data.Vals {
				err = srv.SendRow(i, data.Vals[i])
				if err != nil {
					return
				}

			}
			log.Infof("%d rows written to client", data.Len())

		}
	}
}

// ProcessSQFile loads sq commands from file
func ProcessSQFile(name string) error {
	profile := sqprofile.CreateSQProfile()
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer file.Close()
	//	var lines []string
	scanner := bufio.NewScanner(file)
	//_ = scanner.Text()
	for scanner.Scan() {
		trans := sqtables.BeginTrans(profile, true)
		line := scanner.Text()
		tkns := tokens.Tokenize(line)
		sqfunc := GetDispatchFunc(*tkns)
		_, _, err = sqfunc(trans, tkns)
		if err != nil {
			trans.Rollback()
			return err
		}
		err = trans.Commit()
		if err != nil {
			trans.Rollback()
			return err
		}
	}
	return nil
}
