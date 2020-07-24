package redo

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wilphi/sqsrv/files"

	"github.com/wilphi/sqsrv/sqbin"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/transid"

	log "github.com/sirupsen/logrus"
)

// Redo Package maintains the sql transaction log for the sqsrv

// logFileName - for internal and testing use
var logFileName = "transaction.tlog"

// recoveryFile for internal and testing use
var recoveryFile = "recovery.tlog"

var guardTransProc uint64

var doDirOnce sync.Once

var lazyWrite int // number of seconds between file.Sync - 0 means each write will be synced

// LogMsg is a structure to send a LogStatement across a channel and giving a reponse channel
type LogMsg struct {
	//	stmt    LogStatement
	buffer  []byte
	respond chan error
	id      uint64
}

// TChan is used to send transaction LogStatements to the redo logger
// it also serializes the statements to ensure safe writting to file
type TChan chan LogMsg

var tlog TChan

// logState is for internal or testing only
var logState State

// SetTLog sets the path to the transaction log
// It may be an absolute or relative path. This should only be set once before
// transaction logging is started.
func SetTLog(path string) {
	doDirOnce.Do(func() {
		logFileName = path
	})
}

// SetLazyTlog sets the LazyWrite attribute for the transaction log. This is less data safe in that committed transactions
//  may not be durably written to disk in the case of a failure, but can be an order of magnitude faster.
//  d - number of milliseconds between file.Sync
func SetLazyTlog(d int) {
	lazyWrite = d
}

// Start -
func Start() TChan {
	log.Info("Starting Transaction Logging...")
	tlog = make(TChan, 10)
	go transProc()
	logState.Start()

	return tlog
}

// Stop -
func Stop() {
	log.Info("Stopping Transaction Logging...")
	if !logState.IsStopped() {
		logState.Stop()
		for {
			if atomic.CompareAndSwapUint64(&guardTransProc, 0, 0) {
				break
			}
			// wait for a little bit
			time.Sleep(100 * time.Millisecond)

		}
		close(tlog)
	}
}

// Send -
func Send(s LogStatement) error {
	if !logState.IsStopped() {
		respChan := make(chan error)
		msg := CreateLogMsg(respChan, s)
		tlog <- msg
		err := <-respChan
		return err
	}
	// No err if the Transaction log is not started
	return nil
}

// transProc accepts the LogMessages and durably writes them to disk. Any errors are sent back to the sender
//   this should be called as a new goroutine that will continually accept LogMsgs until the program shutsdown
func transProc() {
	var sent LogMsg
	var ok, isDirty bool
	var lastSync time.Time
	var stack []LogMsg

	// There must be only one transProc running at a time.
	if !atomic.CompareAndSwapUint64(&guardTransProc, 0, 1) {
		panic("transProc is already running")
		//return
	}

	// When function exits remove guard
	defer atomic.StoreUint64(&guardTransProc, 0)

	log.Info("Starting Transaction Logging")
	// If the file doesn't exist, create it. Append to the file as write only
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic(err.Error())
	}
	defer file.Close()
	for {
		// get the log message
		select {
		case sent, ok = <-tlog:
			if !ok {
				if logState.IsStopped() {
					err = file.Sync()
					stack = clearSentStack(stack, err)
					return
				}
				log.Panic("The Transaction log Channel has been closed")

			}
		case <-time.After(time.Millisecond):
			if logState.IsStopped() {
				err = file.Sync()
				stack = clearSentStack(stack, err)
				return
			}

			if time.Now().After(lastSync.Add(time.Duration(lazyWrite) * time.Millisecond)) {
				if isDirty {
					file.Sync()
					log.Trace("Timeout Sync")
					isDirty = false
				}
			}
			continue
		}

		// unpack the log statement and encode it
		encStmt := sqbin.NewCodec(sent.buffer)

		// set the transaction log ID & Length of log
		tID := transid.GetNextID()
		encStmt.Insert(tID, int64(encStmt.Len()))

		n, err := file.Write(encStmt.Bytes())
		// If there was an error put it on the respond channel from the sender
		if err != nil || n != encStmt.Len() {
			sent.respond <- err
			continue
		}

		if lazyWrite == 0 {
			//Sync the file to make sure that the logstatment is durably written to disk
			// if there is another logstatment is waiting then wait to sync until all waiting logstatements have been processed
			// This can double the speed of INSERT/UPDATE heavy code
			sent.id = tID
			stack = append(stack, sent)
			if len(tlog) == 0 {
				err = file.Sync() // Sync is a very expensive operation
				stack = clearSentStack(stack, err)

			}
		} else {
			sent.respond <- err
			if !isDirty {
				lastSync = time.Now()
			}
			isDirty = true
		}
		log.Debugf("%d written to transaction log", tID)

	}
}

func clearSentStack(stack []LogMsg, err error) []LogMsg {
	for _, s := range stack {
		log.Debugf("%d written to transaction log", s.id)
		s.respond <- err
	}
	return stack[:0]
}

// Recovery takes the last backup and the transaction logs to recreate the database to the last transaction
func Recovery(profile *sqprofile.SQProfile) error {
	if !logState.IsStopped() {
		log.Panic("Recovery must occur before the transaction log has been started")
	}
	log.Info("Recovering transaction log")
	start := time.Now()

	isTransLog, err := files.Exists(logFileName)
	if err != nil {
		return err
	}

	if isTransLog {
		// Read the recovery.tlog
		file, err := os.Open(logFileName)
		if err != nil {
			return err
		}
		defer file.Close()

		err = ReadTlog(profile, file)
		if err != nil {
			return err
		}
		log.Infof("Current Transaction ID = %d", transid.GetTransID())
		length := time.Since(start)
		log.Infof("Time spend in recovery: %v", length)
		file.Close()
	}
	return nil
}

// ReadTlog reads the transactionlog and recreates the database changes
func ReadTlog(profile *sqprofile.SQProfile, f io.Reader) error {

	var s LogStatement
	dec := sqbin.NewCodec(nil)
	int64buff := make([]byte, 9)

	for {
		// Get the transID
		n, err := f.Read(int64buff)
		if err != nil || n != 9 {
			if err == io.EOF {
				break
			}
			log.Error("Error Reading recovery transaction log: ", err)
			return err
		}
		dec.Write(int64buff)
		tID := dec.ReadUint64()

		// get marker + len
		n, err = f.Read(int64buff)
		if err != nil || n != 9 {
			if err == io.EOF {
				break
			}
			log.Error("Error Reading recovery transaction log: ", err)
			return err
		}
		dec.Write(int64buff)
		l := dec.ReadInt64()
		buff := make([]byte, l)
		n, err = f.Read(buff)
		if err != nil || n != int(l) {
			log.Error("Error Reading recovery transaction log: ", err)
			return err
		}

		dec.Write(buff)
		s = DecodeStatement(dec)
		if tID <= transid.GetTransID() {
			// statement is already in database
			log.Debugf("Skipping recover statement: %s transid < current Id (%d < %d)", s.Identify(tID), tID, transid.GetTransID())
		} else {
			log.Debug("Attempting to recover statement: ", s.Identify(tID))
			if err := s.Recreate(profile); err != nil {
				log.Error("Unable to recreate from: ", s.Identify(tID))
				return err
			}
			log.Info("Recovered: ", s.Identify(tID))

			// make sure memory reflects current transaction completed
			transid.SetTransID(tID)

		}
	}

	return nil
}
