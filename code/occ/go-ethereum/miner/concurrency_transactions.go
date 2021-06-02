package miner

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/occmanager"
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

type ConcurrentExecutor struct {
	CurrentBlockWorker	*worker
	GasPool	*core.GasPool
	LocalTempState	*state.StateDB
	OCCGlobalManager *occmanager.OCCGlobalManager

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

	var coalescedLogs []*types.Log
	dag := &DAG{
		LatestAccessTx: make(map[[16]byte]uint),
	}



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

	for i := 0; i < len(TxSet); i++ {
		if TxSet[i].To() == nil {
			ContractCreation = append(ContractCreation, TxSet[i])
			TxSet = append(TxSet[:i], TxSet[i+1:]...)
			i = i -1
		}
	}

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
	OCCGlobalManager := &occmanager.OCCGlobalManager{
		CurrentTID: 0,
		GlobalRecords: make(map[[16]byte]*occmanager.Record, 0),
	}

	waitGroutp.Add(ConcurrentThread)

	for i := 0; i < ConcurrentThread; i++ {
		gasP := new(core.GasPool).AddGas(uint64(99999999))
		parent := w.chain.CurrentBlock()
		stateThread, _ := w.chain.StateAt(parent.Root())
		ConcurrentExe := &ConcurrentExecutor{
			CurrentBlockWorker: w,
			GasPool: gasP,
			LocalTempState: stateThread,
			OCCGlobalManager: OCCGlobalManager,
			Dag: dag,
		}
		 go ConcurrentExe.ConcurrentExecuteTxs(TxForThreads[i])

		//fmt.Println("1")
	}

	waitGroutp.Wait()

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

func (c *ConcurrentExecutor) ConcurrentExecuteTxs(txs []*types.Transaction) error {
	runtime.LockOSThread()
	defer waitGroutp.Done()
	//var ValidatePhaseTx [] types.Transaction


	//尝试执行交易
	for _, tx := range txs {
		//fmt.Println(len(txs))
		//start := time.Now()
		for {
			if tx == nil {
				break
			}

			TxState := CanCommit

			if c.CurrentBlockWorker.current.gasPool == nil {
				c.GasPool = new(core.GasPool).AddGas(c.CurrentBlockWorker.current.header.GasLimit)
			}

			//1.执行阶段
			//每次开始执行一个新的交易，都要从全局state同步最新的stateobject，，不要触及db层
			//TempState := state.DeepCopyStateDB(c.CurrentBlockWorker.current.state) //深拷贝
			//tstart := time.Now()
			TempState := c.CurrentBlockWorker.current.state.CopyForThread()	//准备一个线程专用的本地缓存
			//fmt.Println(common.PrettyDuration(time.Since(tstart)))

			occLocalManager := &occmanager.OCCLocalManager{
				ReadSet:                 make(map[[16]byte]*occmanager.Record, 0),
				WriteSet:                make(map[[16]byte]*occmanager.Record, 0),
				OCCGlobalManagerPointer: c.OCCGlobalManager,
			}
			//start1 := time.Now()
			receipt, err := core.ConcurrentApplyTransaction(c.CurrentBlockWorker.chainConfig, c.CurrentBlockWorker.chain, &c.CurrentBlockWorker.coinbase, c.GasPool, TempState, c.CurrentBlockWorker.current.state, c.CurrentBlockWorker.current.header, tx, &c.CurrentBlockWorker.current.header.GasUsed, *c.CurrentBlockWorker.chain.GetVMConfig(), occLocalManager)
			//fmt.Println(getGID(), "=======================", common.PrettyDuration(time.Since(start1)))
			if err != nil {
				fmt.Println(err)
				return err
			}

			GlobalWriteSet := make(map[[16]byte]*occmanager.Record, 0)
			//新增，一次拿到所有record的全局指针，而不是copy
			c.OCCGlobalManager.Mu.Lock()
			for k, _ := range occLocalManager.WriteSet {
				if _, ok := occLocalManager.OCCGlobalManagerPointer.GlobalRecords[k]; !ok {
					r := &occmanager.Record{
						//TransactionID: g.CurrentTID,
						TransactionID: 0,
						Address: common.Address{},
						Key: common.Hash{},
						Lock: false,
					}

					//temp := BytesCombine(address.Bytes(), key.Bytes())
					occLocalManager.OCCGlobalManagerPointer.GlobalRecords[k] = r
					GlobalWriteSet[k] = r
				} else {
					GlobalWriteSet[k] = occLocalManager.OCCGlobalManagerPointer.GlobalRecords[k]
				}
			}
			c.OCCGlobalManager.Mu.Unlock()


			//2.验证阶段
			//对writeset加锁
			var keyList []uint32
			Uint32ToByte16 := make(map[uint32][16]byte)
			for k := range occLocalManager.WriteSet {
				keyList = append(keyList, BytesToUint32(k))
				Uint32ToByte16[BytesToUint32(k)] = k
			}
			sort.Slice(keyList, func(i, j int) bool {
				return keyList[i] < keyList[j]
			})

			Locked := make([][16]byte, 0)

			for _, k := range keyList {
				//key := Uint16ToBytes(k)
				key := Uint32ToByte16[k]
				//v := occLocalManager.WriteSet[key]


				//待解决， 写过不一定是读过
				/*if _, ok := occLocalManager.ReadSet[key]; !ok {
					delete(occLocalManager.WriteSet, key)
					continue
				}*/
				//把写记录放到全面记录

				/*if occLocalManager.OCCGlobalManagerPointer.RecordIsExist(key) == false {
					occLocalManager.OCCGlobalManagerPointer.AddNewRecord(key)
				}*/


				for {
					//fmt.Println("ALock")
					if IsExist(key, Locked) {
						break
					}

					//优化锁机制
					if GlobalWriteSet[key].LockRecord() {
						Locked = append(Locked, key)
						break
					}

					/*if c.OCCGlobalManager.LockRecord(key) {
						Locked = append(Locked, key)
						break
					}*/
				}




			}


			/*for k, _ := range occLocalManager.WriteSet {	//此处应为WriteSet，但是不知为何WriteSet多出一些不该出现的Key，所以此处默认WriteSet 有的ReadSet也有
				if _, ok := occLocalManager.ReadSet[k]; !ok {
					delete(occLocalManager.WriteSet, k)
					continue
				}

				for {
					fmt.Println("ALock")
					if c.OCCGlobalManager.LockRecord(k) {
						break
					}
				}
			}*/

			occLocalManager.AssignedTID = c.OCCGlobalManager.AssignTID()


			c.OCCGlobalManager.Mu.RLock()
			for k, record := range occLocalManager.ReadSet {
				if record.TransactionID != c.OCCGlobalManager.GlobalRecords[k].TransactionID {
					TxState = Abort
				}
				if _, ok := occLocalManager.WriteSet[k]; !ok {
					if c.OCCGlobalManager.GlobalRecords[k].Lock == true {
						TxState = Abort
					}
				}
			}
			c.OCCGlobalManager.Mu.RUnlock()

			if TxState == CanCommit {
				c.CurrentBlockWorker.current.state.FinalizeToGlobal(TempState)
				txIndex := c.CurrentBlockWorker.current.AddTxsAndReceipt(tx, receipt)
				tx.GraphInfor.Key = txIndex

				for _, recor := range GlobalWriteSet {
					//occLocalManager.OCCGlobalManagerPointer.UpdateTID(k, occLocalManager.AssignedTID)
					//锁机制的改进
					recor.UpdateTID(occLocalManager.AssignedTID)
				}


				//锁机制的改进
				c.AddTxToDAG(txIndex, occLocalManager)
				//occLocalManager.ReleaseWriteLock()
				for _,re := range GlobalWriteSet {
					re.UnLockRecord()
				}
				//fmt.Println("done")
				//fmt.Println("Commit")
				//fmt.Println(getGID(),"=========",common.PrettyDuration(time.Since(start)))
				break
			}
			if TxState == Abort {
				//occLocalManager.ReleaseWriteLock()
				for _,re := range GlobalWriteSet {
					re.UnLockRecord()
				}
				//fmt.Println("Abort")
				continue
			}

		}
	}


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

func BytesToUint16(array [16]byte) uint16 {
	var data uint16 =0
	for i:=0;i< len(array);i++  {
		data = data+uint16(uint(array[i])<<uint(8*i))
	}

	return data
}

func BytesToUint32(array [16]byte) uint32 {
	var data uint32 = 0
	for i := 0;i < len(array); i++  {
		data = data + uint32(uint(array[i])<<uint(8*i))
	}

	return data
}

func Uint16ToBytes(n uint16) [16]byte {
	return [16]byte{
		byte(n),
		byte(n >> 8),
	}
}

func (c *ConcurrentExecutor) AddTxToDAG(key uint, OCCLocalMa *occmanager.OCCLocalManager) {
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

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
