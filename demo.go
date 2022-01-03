package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// lock type const
const (
	UNLOCKED int8 = iota
	SHARE_LOCK
	EXCLUSIVE_LOCK
)

var dataSet []*DataSetItem

func init() {
	log.SetOutput(os.Stdout)
	// create random data set
	dataSet = createRandomDataSet()
}

func main() {

	var numWorker = 3

	// modify runtime go max procs
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	println("Set runtime GOMAXPROCS ", numCPU)

	// println runtime info
	fmt.Printf("Start 2pl demo. work num is %v\n", numWorker)
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		fmt.Printf("End 2pl demo. use time %v sec \n", endTime.Unix()-startTime.Unix())
	}()

	// start workers
	var wg sync.WaitGroup
	wg.Add(numWorker)

	for i := 0; i < numWorker; i++ {
		go func() {
			create10000TransactionAndExecute(dataSet)
			wg.Done()
		}()
	}
	wg.Wait()
}

func create10000TransactionAndExecute(set []*DataSetItem) {
	for i := 0; i < 10000; i++ {
		beginNewTransaction(set)
	}
}

func beginNewTransaction(set []*DataSetItem) {
	dataSetLen := len(set)
	for true {
		j := rand.Intn(dataSetLen)
		i := rand.Intn(dataSetLen)
		// FIXME: notice here，I just ignore case: i <= j <= i+2
		if i <= j && j <= i+2 {
			continue
		}

		t := NewTransaction()
		t.operation = &TransactionOperation{
			readSeqA: getReadSeq(i, dataSetLen),
			readSeqB: getReadSeq(i+1, dataSetLen),
			readSeqC: getReadSeq(i+2, dataSetLen),
			writeSeq: j,
		}
		_ = t.Begin()
		break
	}
}

func getReadSeq(seq int, dataSetLen int) int {
	if seq < dataSetLen {
		return seq
	}
	return seq - dataSetLen
}

var globalTransactionCount int64 = 0

func GetTransactionTid() int64 {
	return atomic.AddInt64(&globalTransactionCount, 1)
}

type Transaction struct {

	// state
	state int8

	// operation
	operation *TransactionOperation

	// should_closed
	shouldClosed bool

	// tid
	tid int64

	// timestamp
	ts int64

	// hold lock item
	holdLockItemSeqSlice []int

	// signal channel
	signal chan interface{}

	// mutex protect
	mutex sync.Mutex
}

func NewTransaction() *Transaction {
	return &Transaction{
		shouldClosed: false,
		tid: GetTransactionTid(),

		// timestamp
		ts: time.Now().UnixNano(),

		// hold lock item
		holdLockItemSeqSlice: make([]int, 10),

		// signal channel
		signal: make(chan interface{}),
	}
}

// transaction func
func (t *Transaction) Begin() error {
	log.Printf("Transaction[%v] begin\n", t.tid)
	// check closed mark flag
	if t.shouldClosed {
		errMsg := fmt.Sprintf("Transaction[%v] begin error", t.tid)
		fmt.Println(errMsg)
		return errors.New(errMsg)
	}

	t.mutex.Lock()
	defer func() {
		t.mutex.Unlock()
	}()

	if t.operation == nil {
		errMsg := fmt.Sprintf("Transaction[%v] operation is empty", t.tid)
		fmt.Println(errMsg)
		return errors.New(errMsg)
	}

	// execute operation
	// TODO: handle error
	_ = t.ExecuteOperation(t.operation)
	return nil
}

func (t *Transaction) ExecuteOperation(op *TransactionOperation) error {

	ra := op.readSeqA
	rb := op.readSeqB
	rc := op.readSeqC
	w := op.writeSeq
	/**
	1. try acquire lock
	2. upgrade lock
	3. wait die
	4. wait wakeup
	5. continue
	*/

	itema, err := t.LockAndRead(ra)
	if err != nil {
		log.Printf("Transaction[%v] Lock & Read item [%v] error [%v]", t.tid, ra, err)
		return err
	}
	log.Printf("Transaction[%v] Read item [%v] value [%v]", t.tid, itema.seq, itema.value)

	itemb, err := t.LockAndRead(rb)
	if err != nil {
		log.Printf("Transaction[%v] Lock & Read item [%v] error [%v]", t.tid, rb, err)
		return err
	}
	log.Printf("Transaction[%v] Read item [%v] value [%v]", t.tid, itemb.seq, itemb.value)

	itemc, err := t.LockAndRead(rc)
	if err != nil {
		log.Printf("Transaction[%v] Lock & Read item [%v] error [%v]", t.tid, rc, err)
		return err
	}
	log.Printf("Transaction[%v] Read item [%v] value [%v]", t.tid, itemc.seq, itemc.value)

	sum := itema.value + itemb.value + itemc.value
	err = t.LockAndWrite(w, sum)
	if err != nil {
		log.Printf("Transaction[%v] Write item [%v] value [%v] error [%v]", t.tid, w, sum, err)
		return err
	}
	log.Printf("Transaction[%v] Write item [%v] value [%v] ", t.tid, w, sum)
	return nil
}

func (t *Transaction) LockAndRead(ra int) (*DataSetItem, error) {
	if t.shouldClosed {
		_ = t.ReleaseLocks()
		return nil, errors.New("should close")
	}

	item, err := GetDataSetItem(int64(ra))
	if err != nil {
		return item, err
	}

	// item lock
	item.mutex.Lock()
	defer item.mutex.Unlock()

	transactionHoldShareLock, _ := item.ShareLockHoldContainsTransaction(t.tid)
	alreadyGetExclusiveLock := item.lockState == EXCLUSIVE_LOCK && item.exclusiveLockHoldTransaction.tid == t.tid
	alreadyGetShareLock := item.lockState == SHARE_LOCK && transactionHoldShareLock
	lockAlreadyAcquired := alreadyGetExclusiveLock || alreadyGetShareLock
	if !lockAlreadyAcquired {
		// try wait die
		t.tryWaitDie(item, SHARE_LOCK)
	} else {
		log.Printf("Transaction[%v] already acquire SHARE lock for item[%v] \n", t.tid, item.seq)
	}

	// get lock
	if item.writeTs > t.ts {
		// TODO: abort and failed
		t.abortAndFail()
		errMsg := fmt.Sprintf("Timestamp Protocol. Some younger transaction wrote the value of the item %v", item.seq)
		return nil, errors.New(errMsg)
	} else {
		// TODO: read is safe
		if !lockAlreadyAcquired {
			item.lockState = SHARE_LOCK
			item.shareLockHoldTransactionSlice = append(item.shareLockHoldTransactionSlice, t)
			t.holdLockItemSeqSlice = append(t.holdLockItemSeqSlice, item.seq)
		}
		item.readTs = t.ts
		// TODO: mark succeed
		_ = t.ReleaseLocks()
	}
	return item, nil
}

func (t *Transaction) tryWaitDie(item *DataSetItem, acquireLockType int8) bool {
	if item.lockState == EXCLUSIVE_LOCK {
		curExclusiveTransaction := item.exclusiveLockHoldTransaction
		if t.tid < item.exclusiveLockHoldTransaction.tid {
			// TODO: waiting
			item.waitTransactionSlice = append(item.waitTransactionSlice, t)
			log.Printf("Transaction[%v] waiting for Transaction[%v] \n", t.tid, curExclusiveTransaction.tid)
			<-t.signal
		} else {
			// TODO: abort
			log.Printf("Transaction[%v] abort for Transaction[%v] \n", t.tid, curExclusiveTransaction.tid)
			// TODO: maybe abort and restart
			t.abortAndFail()
		}
		return true
	} else if item.lockState == SHARE_LOCK && acquireLockType == EXCLUSIVE_LOCK {
		minTid := t.tid
		allShareLockHoldTransaction := item.shareLockHoldTransactionSlice
		for _, t := range allShareLockHoldTransaction {
			if t.tid < minTid {
				minTid = t.tid
			}
		}

		if minTid == t.tid {
			// TODO: waiting
			item.waitTransactionSlice = append(item.waitTransactionSlice, t)
			log.Printf("Transaction[%v] waiting for Transaction[%v] \n", t.tid, minTid)
			<-t.signal
		} else {
			// TODO: abort
			log.Printf("Transaction[%v] abort for Transaction[%v] \n", t.tid, minTid)
			// TODO: maybe abort and restart
			t.abortAndFail()
		}
		return true
	}
	return false
}

func (t *Transaction) tryUpgradeToExclusiveLock(item *DataSetItem, acquireLockType int8, sum int) bool {
	shareLockHoldTransactionSlice := item.shareLockHoldTransactionSlice
	if acquireLockType == EXCLUSIVE_LOCK &&
		item.lockState == SHARE_LOCK &&
		len(shareLockHoldTransactionSlice) == 1 &&
		shareLockHoldTransactionSlice[0].tid == t.tid {

		log.Printf("Transaction[%v] acquire upgrade exclusive lock and for item[%v] \n", t.tid, item.seq)

		// tomas write rule
		if item.readTs > t.ts {
			// TODO abort and fail
			t.abortAndFail()
		} else if item.writeTs > t.ts {
			// TODO make succeed skip
		} else {
			// TODO do write
			item.lockState = EXCLUSIVE_LOCK
			item.shareLockHoldTransactionSlice = make([]*Transaction, 10)
			item.exclusiveLockHoldTransaction = t
			item.writeTs = t.ts
			item.value = sum
			// TODO: mark succeed
			_ = t.ReleaseLocks()
		}
		return true
	}
	return false
}

func (t *Transaction) Commit() error {
	return nil
}

func (t *Transaction) ReleaseLocks() error {
	for _, itemSeq := range t.holdLockItemSeqSlice {
		dataItem, err := GetDataSetItem(int64(itemSeq))
		if err != nil {
			log.Printf("Get data set item [%v] null ", itemSeq)
			continue
		}
		t.clearDataSetItemLockStateAndWakeUpTransaction(dataItem)
	}
	return nil
}

func (t *Transaction) clearDataSetItemLockStateAndWakeUpTransaction(dataItem *DataSetItem) {
	if dataItem.lockState == EXCLUSIVE_LOCK && dataItem.exclusiveLockHoldTransaction.tid == t.tid {
		dataItem.lockState = UNLOCKED
		dataItem.exclusiveLockHoldTransaction = nil
	}

	transactionHoldShareLock, seq := dataItem.ShareLockHoldContainsTransaction(t.tid)
	if dataItem.lockState == SHARE_LOCK && transactionHoldShareLock {
		dataItem.RemoveShareLockHoldTransaction(seq)
		if len(dataItem.shareLockHoldTransactionSlice) == 0 {
			dataItem.lockState = UNLOCKED
		}
	}

	if len(dataItem.waitTransactionSlice) > 0 {
		waitTransaction := dataItem.waitTransactionSlice[0]
		waitTransaction.signal <- t.tid
		trimWaitTransaction := dataItem.waitTransactionSlice[1:]
		dataItem.waitTransactionSlice = trimWaitTransaction
	}
}

func (t *Transaction) abortAndFail() {
	_ = t.ReleaseLocks()
	t.shouldClosed = true
}

func (t *Transaction) LockAndWrite(w int, sum int) error {
	item, err := GetDataSetItem(int64(w))
	if err != nil {
		return err
	}

	// item lock
	item.mutex.Lock()
	defer item.mutex.Unlock()

	alreadyGetExclusiveLock := item.lockState == EXCLUSIVE_LOCK && item.exclusiveLockHoldTransaction.tid == t.tid
	acquireLockType := EXCLUSIVE_LOCK
	lockAlreadyAcquired := alreadyGetExclusiveLock
	if !lockAlreadyAcquired {
		// try upgrade lock when acquire lock is exclusive lock
		if t.tryUpgradeToExclusiveLock(item, acquireLockType, sum) {
			// TODO: do write & return
			log.Printf("Transaction[%v] upgrade exclusive lock for item[%v] \n", t.tid, item.seq)
		}

		// try wait die
		t.tryWaitDie(item, acquireLockType)
	} else {
		log.Printf("Transaction[%v] already acquire lock for item[%v] \n", t.tid, item.seq)
	}

	// get lock
	if item.readTs > t.ts {
		// TODO: abort
		t.abortAndFail()
	} else if item.writeTs > t.ts {
		// TODO: skip & mark succeed
		_ = t.ReleaseLocks()

	} else {
		// TODO: write
		if !lockAlreadyAcquired {
			item.lockState = EXCLUSIVE_LOCK
			item.exclusiveLockHoldTransaction = t
			t.holdLockItemSeqSlice = append(t.holdLockItemSeqSlice, item.seq)
		}

		// TODO: write
		item.writeTs = t.ts
		item.value = sum
		// TODO: mark succeed
		_ = t.ReleaseLocks()
	}
	return nil
}

type TransactionOperation struct {

	// S(j)	  S(i)		S(i+1)	  S(i+2)
	writeSeq, readSeqA, readSeqB, readSeqC int
}

func createRandomDataSet() []*DataSetItem {
	dataSetLen := 10000
	dataSet := make([]*DataSetItem, 0, dataSetLen)
	rand.Seed(time.Now().Unix())
	for i := 0; i < dataSetLen; i++ {
		dataSet = append(dataSet, &DataSetItem{
			// initial value just set range 100
			seq:                           i,
			value:                         rand.Intn(100),
			readTs:                        0,
			writeTs:                       0,
			lockState:                     UNLOCKED,
			shareLockHoldTransactionSlice: make([]*Transaction, 0, 10),
			waitTransactionSlice:          make([]*Transaction, 0, 10),
		})
	}
	return dataSet
}

func GetDataSetItem(seq int64) (*DataSetItem, error) {
	item := dataSet[seq]
	if item == nil {
		errMsg := fmt.Sprintf("Get item %v nil", seq)
		return nil, errors.New(errMsg)
	}
	return item, nil
}

type DataSetItem struct {

	// seq
	seq int

	// value
	value int

	// read ts
	readTs int64

	// write ts
	writeTs int64

	// lock state
	lockState int8

	// exclusive lock hold transaction
	exclusiveLockHoldTransaction *Transaction

	// share lock hold
	shareLockHoldTransactionSlice []*Transaction

	// wait transaction queue
	waitTransactionSlice []*Transaction

	// mutex
	mutex sync.Mutex
}

func (d *DataSetItem) RemoveShareLockHoldTransaction(seq int) {
	d.shareLockHoldTransactionSlice = append(d.shareLockHoldTransactionSlice[:seq], d.shareLockHoldTransactionSlice[seq+1:]...)
}

func (d *DataSetItem) ShareLockHoldContainsTransaction(tid int64) (bool, int) {
	for seq, t := range d.shareLockHoldTransactionSlice {
		if t.tid == tid {
			return true, seq
		}
	}
	return false, -1
}
