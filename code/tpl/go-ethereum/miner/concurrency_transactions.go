package miner

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/twoplmanager"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

//交易执行状态
const (
	UnExecuted = 0	//未执行
	CanCommit = 1	//已执行未提交
	Abort = 2	//已执行已提交

	ConcurrentThread = 20

	TxSize = 1024

)
const (
	readLock = 2
	writelock = 3
)

type ConcurrentExecutor struct {
	CurrentBlockWorker	*worker
	GasPool	*core.GasPool
	LocalTempState	*state.StateDB
	TPLGlobalManager *twoplmanager.TwoPLGlobalManger

	Dag *DAG
}

type DAG struct {
	Vertexes []*types.Transaction
	LatestAccessTx map[[16]byte]uint

	Mu sync.Mutex
}


var waitGroutp = sync.WaitGroup{}

func (w *worker) commitTransactionsConcurrently(txs map[common.Address]types.Transactions, coinbase common.Address, interrupt *int32) bool {
	// Short circuit if current is nil
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.current.header.GasLimit)
	}

	//用于确定性重放的DAG
	var coalescedLogs []*types.Log
	dag := &DAG{
		LatestAccessTx: make(map[[16]byte]uint),
	}


	//开始执行交易
	fmt.Printf("********************	commitTransactionsConcurrently == start 	********************\n")
	/*for from, accTxs := range txs {
		for index, tx := range accTxs {
			fmt.Printf("index: %d\nfrom: %+v\ntransaction: %+v\n", index, from, tx)
		}
	}*/

	var TxSet []*types.Transaction		//确定的交易顺序
	for _, accTxs := range txs {
		TxSet = append(TxSet, accTxs...)
	}

	var TxForThreads []types.Transactions
	var ContractCreation [] *types.Transaction

	//把合约创建交易分离出来单独执行
	for i := 0; i < len(TxSet); i++ {
		if TxSet[i].To() == nil {
			ContractCreation = append(ContractCreation, TxSet[i])
			TxSet = append(TxSet[:i], TxSet[i+1:]...)
			i = i -1
		}
	}
	//执行合约创建交易
	for _, tx := range ContractCreation {
		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)
		receipt, _ := core.ApplyTransaction(w.chainConfig, w.chain, &coinbase, w.current.gasPool, w.current.state, w.current.header, tx, &w.current.header.GasUsed, *w.chain.GetVMConfig())
		w.current.txs = append(w.current.txs, tx)
		w.current.receipts = append(w.current.receipts, receipt)
		w.current.tcount++
	}

	//把TxSet的交易平均分配给每一个线程
	for i := 0; i <= ConcurrentThread; i++ {
		if i != ConcurrentThread {
			TxForThreads = append(TxForThreads, TxSet[i * (len(TxSet) / ConcurrentThread) : (i + 1) * (len(TxSet) / ConcurrentThread) ])
		} else if i == ConcurrentThread && len(TxSet) % ConcurrentThread != 0 {
			//TxForThreads = append(TxForThreads, TxSet[i * (len(TxSet) / ConcurrentThread) :])
			for index, tx := range TxSet[i * (len(TxSet) / ConcurrentThread) :] {
				temp := make([]*types.Transaction, 0, len(TxForThreads[index % ConcurrentThread]) + 1)
				for _ , t := range TxForThreads[index % ConcurrentThread] {
					temp = append(temp, t)
				}

				//temp = TxForThreads[index % ConcurrentThread]
				temp = append(temp, tx)
				TxForThreads[index % ConcurrentThread] = temp
			}
		}
	}
	/*OCCGlobalManager := &occmanager.OCCGlobalManager{
		CurrentTID: 0,
		GlobalRecords: make(map[[16]byte]*occmanager.Record, 0),
	}*/

	//记录全局的已申请的锁
	TPLGlobalManager := &twoplmanager.TwoPLGlobalManger{
		GlobalRecords: make(map[[16]byte]*twoplmanager.Record, 0),
	}

	waitGroutp.Add(ConcurrentThread)

	//初始化并启动多个线程
	for i := 0; i < ConcurrentThread; i++ {
		gasP := new(core.GasPool).AddGas(uint64(99999999))
		parent := w.chain.CurrentBlock()
		stateThread, _ := w.chain.StateAt(parent.Root())
		ConcurrentExe := &ConcurrentExecutor{
			CurrentBlockWorker: w,
			GasPool: gasP,
			LocalTempState: stateThread,
			TPLGlobalManager: TPLGlobalManager,
			Dag: dag,
		}
		go ConcurrentExe.ConcurrentExecuteTxs(TxForThreads[i])

		//fmt.Println("1")
	}

	//等待协程执行完成
	waitGroutp.Wait()

	/*w.current.state.FinalizeLock.Lock()
	w.current.state.Finalise(true)
	w.current.state.FinalizeLock.Unlock()*/

	/*for _, re := range w.current.receipts {
		fmt.Println(*re)
	}*/

	/*for _, re := range w.current.txs {
		fmt.Println(re.GraphInfor)
	}*/

	fmt.Printf("********************	commitTransactionsConcurrently == end 	********************\n")
	fmt.Printf("ConcurrentThread --------------> %+v\n", ConcurrentThread)





	// In the following three cases, we will interrupt the execution of the transaction.
	// (1) new head block event arrival, the interrupt signal is 1
	// (2) worker start or restart, the interrupt signal is 1
	// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
	// For the first two cases, the semi-finished work will be discarded.
	// For the third case, the semi-finished work will be submitted to the consensus engine.
	if interrupt != nil && atomic.LoadInt32(interrupt) != commitInterruptNone {
		// Notify resubmit loop to increase resubmitting interval due to too frequent commits.
		if atomic.LoadInt32(interrupt) == commitInterruptResubmit {
			ratio := float64(w.current.header.GasLimit-w.current.gasPool.Gas()) / float64(w.current.header.GasLimit)
			if ratio < 0.1 {
				ratio = 0.1
			}
			w.resubmitAdjustCh <- &intervalAdjust{
				ratio: ratio,
				inc:   true,
			}
		}
		return atomic.LoadInt32(interrupt) == commitInterruptNewHead
	}

	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the worker will regenerate a mining block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		w.pendingLogsFeed.Send(cpy)
	}
	// Notify resubmit loop to decrease resubmitting interval if current interval is larger
	// than the user-specified one.
	if interrupt != nil {
		w.resubmitAdjustCh <- &intervalAdjust{inc: false}
	}
	return false

}


//线程执行交易入口
func (c *ConcurrentExecutor) ConcurrentExecuteTxs(txs []*types.Transaction) error {
	runtime.LockOSThread()	//协程独占线程
	defer waitGroutp.Done()
	//var ValidatePhaseTx [] types.Transaction

	for _, tx := range txs {
		//处理一个Tx
		//start := time.Now()
		for {
			TxState := CanCommit	//初始化为可提交状态

			//预先申请锁，防止死锁,提前按照指定顺序申请锁
			writeRecordSet := make(map[[16]byte]*twoplmanager.Record, 0)
			var keyList []uint32
			Uint32ToByte16 := make(map[uint32][16]byte, 0)

			for _, callInfor := range tx.ConcurrencyInformation.CallInformation {
				for _, writeset := range callInfor.WriteSet{
					Elemkey := writeset.ElemKey
					key := crypto.Keccak256Hash([]byte(Elemkey))
					address := callInfor.Address
					record := c.TPLGlobalManager.GetRecord(address, key)
					temp := twoplmanager.BytesCombine(address.Bytes(), key.Bytes())
					writeRecordSet[md5.Sum(temp)] = record
				}
			}

			//申请写锁
			//为了方便，直接申请写锁，因为我们的实验设置是对同一个数据进行读和写，读锁必然最终升级为写锁
			for k := range writeRecordSet {
				keyList = append(keyList, BytesToUint32(k))
				Uint32ToByte16[BytesToUint32(k)] = k
			}
			sort.Slice(keyList, func(i, j int) bool {
				return keyList[i] < keyList[j]
			})
			Locked := make([][16]byte, 0)
			for _, k := range keyList {
				key := Uint32ToByte16[k]
				for {

					if IsExist(key, Locked) {
						break
					}
					//result, _ := c.TPLGlobalManager.ApplyForLock(writelock, writeRecordSet[key].Address, writeRecordSet[key].Key) //申请写锁
					//record := c.TPLGlobalManager.GetRecord(writeRecordSet[key].Address, writeRecordSet[key].Key)
					//result := record.LockRecord(writelock)
					result := writeRecordSet[key].LockRecord(writelock)
					if result == true {
						Locked = append(Locked, key)
						break
					}
				}
			}

			if c.CurrentBlockWorker.current.gasPool == nil {
				c.GasPool = new(core.GasPool).AddGas(c.CurrentBlockWorker.current.header.GasLimit)
			}

			TwoPLManager := &twoplmanager.LocalManager{
				ReadSet:                 make(map[[16]byte]*twoplmanager.Record, 0),
				WriteSet:                make(map[[16]byte]*twoplmanager.Record, 0),
				LockedRecord: make(map[[16]byte]*twoplmanager.Record, 0),
				TwoPLGlobalMangerPointer:	c.TPLGlobalManager,
			}

			//1.执行并申请锁阶段
			//直接操作公共的stateDB

			TempState := c.CurrentBlockWorker.current.state.CopyForThread()
			//start1 := time.Now()
			receipt, err, RollbackFlag := core.ConcurrentApplyTransaction(c.CurrentBlockWorker.chainConfig, c.CurrentBlockWorker.chain, &c.CurrentBlockWorker.coinbase, c.GasPool, TempState, c.CurrentBlockWorker.current.state, c.CurrentBlockWorker.current.header, tx, &c.CurrentBlockWorker.current.header.GasUsed, *c.CurrentBlockWorker.chain.GetVMConfig(),c.TPLGlobalManager, TwoPLManager)
			//fmt.Println("=======================",common.PrettyDuration(time.Since(start1)))
			//回滚的情况
			if err  !=  nil {
				fmt.Println(err)
				TxState = Abort
			}
			if RollbackFlag == true {
				TxState = Abort
			}

			//用于DAG的构造
			for k, record := range writeRecordSet {
				TwoPLManager.WriteSet[k] = record
			}

			if TxState == CanCommit {
				//把Tx的Receipt加入对应切片，更新DAG
				c.CurrentBlockWorker.current.Lock.Lock()
				txIndex := c.CurrentBlockWorker.current.AddTxsAndReceipt(tx, receipt)
				tx.GraphInfor.Key = txIndex
				c.AddTxToDAG(txIndex, TwoPLManager)
				c.CurrentBlockWorker.current.Lock.Unlock()

				//3.释放锁
				for _, record := range writeRecordSet {
					record.UnLockRecord()
				}
				//fmt.Println(getGID(),"Tx Done")
				//fmt.Println(common.PrettyDuration(time.Since(start)))
				break
			}

			if TxState == Abort {
				fmt.Println("abort")
				continue
			}
		}
	}

	//线程处理完所有事务，退出
	//fmt.Println("****************Done*********************")
	return nil
}

func IsExist(value [16]byte, sli [][16]byte) bool {
	for _, v := range sli {
		if v == value {
			return true
		}
	}
	return false
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}


func (c *ConcurrentExecutor) AddTxToDAG(key uint, OCCLocalMa *twoplmanager.LocalManager) {
	c.Dag.Mu.Lock()
	defer c.Dag.Mu.Unlock()

	txSet := c.CurrentBlockWorker.current.txs
	tx := txSet[key]

	c.Dag.Vertexes = append(c.Dag.Vertexes, tx)

	for k, _ := range OCCLocalMa.WriteSet {
		if _, ok := c.Dag.LatestAccessTx[k]; !ok {
			c.Dag.LatestAccessTx[k] = key
		} else {
			lastKey := c.Dag.LatestAccessTx[k]
			addEdge(txSet[lastKey], txSet[key])
			c.Dag.LatestAccessTx[k] = key
		}
	}

}

func addEdge(from *types.Transaction, to *types.Transaction) {
	fromKey := from.GraphInfor.Key
	tokey := to.GraphInfor.Key

	if !IsExitKey(from.GraphInfor.ChildNodeIndex, tokey) && fromKey != tokey {
		from.GraphInfor.ChildNodeIndex = append(from.GraphInfor.ChildNodeIndex, tokey)
		/*fmt.Println("Add edge____________________")
		fmt.Println(fromKey)
		fmt.Println(tokey)*/
	}

	if !IsExitKey(to.GraphInfor.ParentNodeIndex, fromKey) && fromKey != tokey {
		to.GraphInfor.ParentNodeIndex = append(to.GraphInfor.ParentNodeIndex, fromKey)
		/*fmt.Println("Add edge____________________")
		fmt.Println(fromKey)
		fmt.Println(tokey)*/
	}
}

func IsExitKey(sli []uint, key uint) bool {
	for _, v := range sli {
		if v == key {
			return true
		}
	}

	return false
}

func BytesToUint32(array [16]byte) uint32 {
	var data uint32 = 0
	for i := 0;i < len(array); i++  {
		data = data + uint32(uint(array[i])<<uint(8*i))
	}

	return data
}
