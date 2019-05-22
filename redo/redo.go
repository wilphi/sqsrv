package redo

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

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

// LogMsg is a structure to send a LogStatement across a channel and giving a reponse channel
type LogMsg struct {
	stmt    LogStatement
	respond chan error
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

	// There must be only one transProc running at a time.
	if !atomic.CompareAndSwapUint64(&guardTransProc, 0, 1) {
		log.Error("transProc is already running")
		return
	}

	log.Info("Starting Transaction Logging")
	// If the file doesn't exist, create it. Append to the file as write only
	file, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//	log.Fatal(err)
		log.Panic(err)
	}

	defer file.Close()
	for {
		// get the log message
		sent, ok := <-tlog
		if !ok {
			log.Fatal("The Transaction log Channel has been closed")
		}

		// unpack the log statement and encode it
		stmt := sent.stmt

		// set the transaction log ID
		stmt.SetID(transid.GetNextID())

		encStmt := stmt.Encode()
		encStmt.InsertInt64(int64(encStmt.Len()))

		n, err := file.Write(encStmt.Bytes())
		// If there was an error put it on the respond channel from the sender
		if err != nil || n != encStmt.Len() {
			sent.respond <- err
			continue
		}

		//Sync the file to make sure that the logstatment is durably written to disk
		err = file.Sync()
		log.Debugf("%d written to transaction log", stmt.GetID())
		sent.respond <- err
	}
}

// Recovery takes the last backup and the transaction logs to recreate the database to the last transaction
func Recovery(profile *sqprofile.SQProfile) error {
	if !logState.IsStopped() {
		log.Panic("Recovery must occur before the transaction log has been started")
	}
	log.Info("Recovering transaction log")
	start := time.Now()

	isTransLog, err := fileExists(logFileName)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	/*
		isRecoveryLog, err := fileExists(recoveryFile)
		if err != nil {
			return err
		}

		if isTransLog && isRecoveryLog {
			errstr := "Both the transaction log and recovery log exist"
			log.Error(errstr)
			return sqerr.New(errstr)
		}
		if isTransLog {
			// Move transaction log to recovery log
			if err := os.Rename(fileName, recoveryFile); err != nil {
				log.Error(fmt.Sprintf("Unable to rename %q to recovery.tlog Error:%s \n", fileName, err))
				return err
			}
			isRecoveryLog = true

		}
		//Get the last backup
		//return sqerr.NewInternal("Backup does not exist")
		//
	*/

	//	if isRecoveryLog {

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
		log.Info(fmt.Sprintf("Time spend in recovery: %v", length))
		file.Close()
		// delete file
		/*
			err = os.Remove(recoveryFile)
			if err != nil {
				return nil
			}
		*/
	}
	return nil
}

// ReadTlog reads the transactionlog and recreates the database changes
func ReadTlog(profile *sqprofile.SQProfile, f io.Reader) error {

	var s LogStatement
	dec := sqbin.NewCodec(nil)
	int64buff := make([]byte, 9)

	for {
		// get marker + len
		n, err := f.Read(int64buff)
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
		if s.GetID() <= transid.GetTransID() {
			// statement is already in database
			log.Debugf("Skipping recover statement: %s transid < current Id (%d < %d)", s.Identify(), s.GetID(), transid.GetTransID())
		} else {
			log.Debug("Attempting to recover statement: ", s.Identify())
			if err := s.Recreate(profile); err != nil {
				log.Error("Unable to recreate from: ", s.Identify())
				return err
			}
			log.Info("Recovered: ", s.Identify())

			// make sure memory reflects current transaction completed
			transid.SetTransID(s.GetID())

		}
	}

	return nil
}

// Tests to see if file exists and has a size >0
func fileExists(fileName string) (bool, error) {
	exists := true
	fs, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return false, err
		}
	} else {
		exists = fs.Size() != 0
	}
	return exists, nil
}
