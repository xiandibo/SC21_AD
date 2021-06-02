package miner

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/miner/queue"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

//交易执行状态
const (
	UnExecuted = 0	//未执行
	Executed = 1	//已执行未提交
	Completed = 2	//已执行已提交
	UnCompleted = 3

	MainTransaction = 3
	CallTransaction = 4

	ConcurrentThread = 20
	ThreadForMainTx = 12

	TxSize = 1024
)
/*
type TxData struct {
	AccountNonce uint64
	Price        *big.Int
	GasLimit     uint64
	Recipient    *common.Address // nil means contract creation
	Amount       *big.Int
	Payload      []byte

	// Signature values
	V *big.Int
	R *big.Int
	S *big.Int

	// This is only used when marshaling to JSON.
	Hash *common.Hash
}

type CallCtx struct {
	Memory   *vm.Memory
	Stack    *vm.Stack
	Rstack   *vm.ReturnStack
	Contract *vm.Contract
}*/

//存储MainTx的运行时状态，以及调用结果,与EVM的附加状态相对应
type MainRuntimeArgs struct {
	//CallCtx		CallCtx

	Memory   *vm.Memory
	Stack    *vm.Stack
	Rstack   *vm.ReturnStack
	Contract *vm.Contract

	ToAddr		common.Address
	Args		[]byte
	Gas			uint64
	Value		*big.Int
	PC			uint64

	ReturnData []byte
	ReturnGas	uint64
	err		error

	PendingFlag bool

	Halts bool

	MainOrCall	bool

	CallType	uint

}

//并发执行使用的调用交易结构
type CallTx struct {
	ReadSet      	map[string]string
	WriteSet     	map[string]string
	CallAddr        common.Address

	TxState			uint	//标志交易执行的状态	UnExecuted = 0	=>未执行 	Executed = 1	=>已执行未提交 	Completed = 2	=>已执行已提交
	CanAbort		bool	//标志此CallTx是否可能被abort
	DataRVP			uint
	CommitFlag		bool
	InputData		[]byte
	//PointerOfMainTx	*MainTx
	PointerOfMainTx	*ConcurrentTransaction
	//TxIndex			uint	//交易在一个批次中的位置
	//Data			TxData

	//定义通道，用于与MainTx传输数据

}

//主交易结构
type MainTx struct {
	//Data TxData
	// caches
	//Hash atomic.Value
	//Size atomic.Value
	//From atomic.Value

	CallReturnSignal bool	//调用返回标志
	CommitPoint uint		//预设提交点
	TxState uint			//交易状态
	CurrentSegment uint		//目前运行点
	//PCCounter	uint		//保存上一次PC位置
	//ReturnData []byte		//CALL返回数据

	//MainRTArgs	*MainRuntimeArgs //保存合约运行状态
	EVMState			*vm.EVM
	// TxIndex		uint		//交易在一个批次中的位置
	UsedGas		*uint64

	//PointerOfCallTxs []*CallTx
	PointerOfCallTxs []*ConcurrentTransaction

	//定义通道，用于与CallTx传输数据

}

type ConcurrentTransaction struct {
	MainOrCall			uint		//标志主事务或调用事务
	MainTx				MainTx		//主交易信息
	CallTx				CallTx		//调用交易信息
	Transaction			*types.Transaction		//原始交易信息
	//TxData				TxData
	TxIndex		uint		//交易在一个批次中的位置 //交易在交易集中的位置
}
/*
type environmentConcurrent struct {
	signer types.Signer

	state     state.StateDB // apply state changes here

	tcount    int            // tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions

	header   *types.Header
	//txs      []*types.Transaction
	receipts []*types.Receipt
}*/

type ConcurrentTxExecutor struct {
	//state     state.StateDB		//每一个并发执行线程一开始都保存区块刚开始的数据库状态副本
	chainConfig        *params.ChainConfig
	chain              *core.BlockChain
	//current      environmentConcurrent
	//signer types.Signer

	State     *state.StateDB // apply state changes here

	tcount    int            // tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions
	GasUsed   uint64

	header   *types.Header
	//txs      []*types.Transaction
	//receipts []*types.Receipt
	receipts map[uint]*types.Receipt

	coinbase common.Address
}

var waitGroutp = sync.WaitGroup{}

/*
@名称	commitTransactionsConcurrently
@描述	并发执行交易入口，把原始交易按照携带读写集信息划分为MainTx 和 CallTx，其中如何分配这些交易还未确定
*/
func (w *worker) commitTransactionsConcurrently(txs map[common.Address]types.Transactions, coinbase common.Address, interrupt *int32) bool {
	// Short circuit if current is nil
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.current.header.GasLimit)
	}

	var coalescedLogs []*types.Log



	fmt.Printf("********************	commitTransactionsConcurrently == start 	********************\n")



	//把原来根据发送者地址排列的事务转化成确定顺序的事务集合
	var TxSet []*types.Transaction		//确定的交易顺序
	for _, accTxs := range txs {
		TxSet = append(TxSet, accTxs...)
	}

	/*if len(TxSet) < 400 {
		fmt.Printf("********************	Not Enough Tx == end 	********************\n")
		return true
	} else {
		TxSet = TxSet[: 400]
	}*/

	var SeralizeTxSet []*ConcurrentTransaction	//只能串行执行的事务


	TxAddrMap := make(map[common.Address]types.Transactions, 0)
	var CreatTx []*types.Transaction
	var indexOfAdd []common.Address

	for _, tx := range TxSet {
		if tx.To() != nil {
			if _, ok := TxAddrMap[*tx.To()]; !ok {
				indexOfAdd = append(indexOfAdd, *tx.To())
			}
			TxAddrMap[*tx.To()] = append(TxAddrMap[*tx.To()], tx)
		} else {
			CreatTx = append(CreatTx, tx)
		}
	}

	/*for k, _ := range TxAddrMap {
		indexOfAdd = append(indexOfAdd, k)
	}*/

	var TxSetCopy []*types.Transaction
	if len(TxAddrMap) != 0 {
		for i := 0; i < len(TxSet); {
			for index, _ := range indexOfAdd {
				k := indexOfAdd[index]
				v := TxAddrMap[k]
				if len(v) != 0 {
					TxSetCopy = append(TxSetCopy, v[0])
					TxAddrMap[k] = v[1:]
					i++
				}
			}
		}
	}

	TxSet = append(TxSetCopy, CreatTx...)




	//用于分配，把事务集分配给多个线程
	TxsForThread := make(map[uint][]*ConcurrentTransaction)	//按照线程标识的事务集合,每个线程对应一个事务集,并发执行
	AddrForThread := make(map[common.Address]uint)	//记录已经分配给线程的合约地址，保证相同的地址分到同一个线程集合

	var indexOfThread uint = 0	//用于循环分配Tx给线程，标志当前新地址合约应当分配的线程
	var indexOfMainTx uint = 0
	var flagAddrExit = false	//标志分配时遇到新的合约地址，分配给下一个线程

	//从每个提交的交易中提取出并发信息
	for index, tx := range TxSet {
		flagAddrExit = false
		txdata, _, _, _ := types.GetInformation(tx)
		/*txdata, hash, size, from := types.GetInformation(tx)
		txData := TxData{
			AccountNonce: txdata.AccountNonce,
			Price: txdata.Price,
			GasLimit: txdata.GasLimit,
			Recipient: txdata.Recipient,
			Amount: txdata.Amount,
			Payload: txdata.Payload,
			V: txdata.V,
			R: txdata.R,
			S: txdata.S,
			Hash: txdata.Hash,
		}*/

		//特殊的无法并发执行的交易：创建合约交易，以及没有调用信息的交易，将来或许包括转帐交易
		if txdata.Recipient == nil || len(tx.ConcurrencyInformation.CallInformation) == 0 {
			concurrencyTxMain := &ConcurrentTransaction{
				MainOrCall: MainTransaction,
				Transaction: tx,
				MainTx: MainTx{
					CallReturnSignal: true,
					CommitPoint: 0,
					TxState: UnCompleted,
					CurrentSegment: 0,
					//PointerOfCallTxs: make([]*ConcurrentTransaction,0),
					EVMState: &vm.EVM{
						PC: 0,
					},
					UsedGas: new(uint64),
				},
				TxIndex: uint(index),
			}
			//ConcurrencyTxSet = append(ConcurrencyTxSet, concurrencyTxMain)
			//单独的串行执行交易集，在所有并发行为完成后，再执行
			SeralizeTxSet = append(SeralizeTxSet, concurrencyTxMain)
			continue
		}

		//构造MainTx
		concurrencyTxMain := &ConcurrentTransaction{
			MainOrCall: MainTransaction,
			Transaction: tx,
			MainTx: MainTx{
				CallReturnSignal: true,
				CommitPoint: tx.ConcurrencyInformation.CommitPoint,
				TxState: UnCompleted,
				CurrentSegment: 0,
				PointerOfCallTxs: make([]*ConcurrentTransaction,0),
				EVMState: &vm.EVM{
					PC: 0,
				},
				UsedGas: new(uint64),
			},
			TxIndex: uint(index),
		}
		//分配MainTx
		//ConcurrencyTxSet = append(ConcurrencyTxSet, concurrencyTxMain)
		if _, ok := AddrForThread[*(txdata.Recipient)]; ok {
			flagAddrExit = true
			TxsForThread[AddrForThread[*(txdata.Recipient)]] = append(TxsForThread[AddrForThread[*(txdata.Recipient)]], concurrencyTxMain)
		}
		/*for addr := range AddrForThread {
			if addr == *(txdata.Recipient) {
				flagAddrExit = true
				TxsForThread[AddrForThread[*(txdata.Recipient)]] = append(TxsForThread[AddrForThread[*(txdata.Recipient)]], concurrencyTxMain)
				break
			}
		}*/
		/*if flagAddrExit == false {
			AddrForThread[*(txdata.Recipient)] = indexOfMainTx
			TxsForThread[indexOfMainTx] = append(TxsForThread[indexOfMainTx], concurrencyTxMain)
			indexOfMainTx = (indexOfMainTx + 1) % ThreadForMainTx
		}*/
		if flagAddrExit == false {
			AddrForThread[*(txdata.Recipient)] = indexOfMainTx
			TxsForThread[indexOfMainTx] = append(TxsForThread[indexOfMainTx], concurrencyTxMain)
			indexOfMainTx = (indexOfMainTx + 1) % ConcurrentThread
		}


		//构造CallTx
		for idex, CallOption := range tx.ConcurrencyInformation.CallInformation {
			flagAddrExit = false
			canAbortFlag := uint(idex) < tx.ConcurrencyInformation.CommitPoint
			concurrencyTxCall := &ConcurrentTransaction{
				MainOrCall: CallTransaction,
				Transaction: tx,
				//TxData: txData,
				CallTx : CallTx{
					CallAddr: CallOption.Address,
					TxState: UnExecuted,
					CanAbort: canAbortFlag,
					DataRVP: 1,
					PointerOfMainTx: concurrencyTxMain,
					ReadSet: make(map[string]string),
					WriteSet: make(map[string]string),
				},
				TxIndex: uint(index),
			}
			for _, readset := range CallOption.ReadSet{
				concurrencyTxCall.CallTx.ReadSet[readset.VarName] = readset.ElemKey
			}
			for _, writeset := range CallOption.WriteSet{
				concurrencyTxCall.CallTx.WriteSet[writeset.VarName] = writeset.ElemKey
			}
			//ConcurrencyTxSet = append(ConcurrencyTxSet, concurrencyTxCall)
			concurrencyTxMain.MainTx.PointerOfCallTxs = append(concurrencyTxMain.MainTx.PointerOfCallTxs, concurrencyTxCall)

			//分配CallTx
			if _, ok := AddrForThread[concurrencyTxCall.CallTx.CallAddr]; ok {
				flagAddrExit = true
				TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]] = append(TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]], concurrencyTxCall)
			}

			/*for addr := range AddrForThread {
				if addr == concurrencyTxCall.CallTx.CallAddr {
					flagAddrExit = true
					TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]] = append(TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]], concurrencyTxCall)
					break
				}
			}*/
			/*if flagAddrExit == false {
				AddrForThread[concurrencyTxCall.CallTx.CallAddr] = indexOfThread + ThreadForMainTx
				TxsForThread[indexOfThread + ThreadForMainTx] = append(TxsForThread[indexOfThread + ThreadForMainTx], concurrencyTxCall)
				indexOfThread = (indexOfThread + 1) % (ConcurrentThread - ThreadForMainTx)
			}*/
			if flagAddrExit == false {
				AddrForThread[concurrencyTxCall.CallTx.CallAddr] = indexOfThread
				TxsForThread[indexOfThread] = append(TxsForThread[indexOfThread], concurrencyTxCall)
				indexOfThread = (indexOfThread + 1) % ConcurrentThread
			}
		}

	}


	/*
	for _, ctx := range ConcurrencyTxSet{
		fmt.Printf("ConcurrencyTxSet -------------->\n %+v\n", ctx)
	}*/
	/*for threadInd, txs := range TxsForThread{
		fmt.Printf("threadIndex ======> %+v\n", threadInd)
		for _, ctx := range txs{
			fmt.Printf("ConcurrencyTxSet -------------->\n %+v\n", ctx)
		}
	}*/

	//fmt.Printf("%+v\n", runtime.NumCPU())
	//fmt.Printf("%+v\n", runtime.NumGoroutine())

	//保存并发执行对象Executor，每一个主要维护独立的状态数据库
	var ExecutorSet []*ConcurrentTxExecutor

	for i := 0; i < ConcurrentThread; i++ {
		//对每一个线程执行器，都初始化相同的state，目的是在最后汇总 stateObjects
		gasP := new(core.GasPool).AddGas(uint64(99999999))
		parent := w.chain.CurrentBlock()
		stateThread, _ := w.chain.StateAt(parent.Root())
		envCon := &ConcurrentTxExecutor{
			chainConfig: w.chainConfig,
			chain: w.chain,
			tcount: 0,
			GasUsed:0,
			//gasPool: w.current.gasPool,
			gasPool: gasP,
			header: w.current.header,
			coinbase: coinbase,
			State: stateThread,
			receipts: make(map[uint]*types.Receipt),
		}
		ExecutorSet = append(ExecutorSet, envCon)

	}

	waitGroutp.Add(ConcurrentThread)	//用于等待所有协程完成
	//整合txcount和header的gasused收据
	for i := 0; i < ConcurrentThread; i++ {
		go ExecutorSet[i].ConcurrentProcessTxs(TxsForThread[uint(i)])
	}


	//等待每一个线程结束运行
	waitGroutp.Wait()

	//finalize每一个线程，把每一个Executor的对象整合到worker对象中
	length := len(TxSet)
	gasU := new(uint64)
	var receps = make([]*types.Receipt, length)

	//遍历每一个线程的执行结果
	for i := 0; i < ConcurrentThread; i++ {
		/*if ExecutorSet[i].chainConfig.IsByzantium(ExecutorSet[i].header.Number) {
			ExecutorSet[i].State.FinaliseForThreads(true)	//把dirty的stateobject添加到pending,与之前不同的是，不删除dirty的journal
		}*/
		ExecutorSet[i].State.FinaliseForThreads(true)	//把dirty的stateobject添加到pending,与之前不同的是，不删除dirty的journal
		w.current.state.AddStateToWorker(ExecutorSet[i].State)		//把线程的state添加到worker的state中
		for k, v := range ExecutorSet[i].receipts {
			receps[k] = v
			*gasU += v.GasUsed
		}
		//*gasU += ExecutorSet[i].GasUsed
	}

	//串行执行无法并行的交易
	for _, v := range SeralizeTxSet {
		receipt, _ := ApplyCTxForMainTx(w.chainConfig, w.chain, &w.coinbase, w.current.gasPool, w.current.state, w.current.header, v, v.MainTx.UsedGas, *w.chain.GetVMConfig())
		w.current.state.Finalise(true)
		receps[v.TxIndex] = receipt
		*gasU += receipt.GasUsed
	}


	//计算receipt的累计GAS
	for i := 0; i < len(receps); i++ {
		if i == 0 {
			receps[i].CumulativeGasUsed = receps[i].GasUsed
			continue
		}
		receps[i].CumulativeGasUsed = receps[i-1].CumulativeGasUsed + receps[i].GasUsed
	}

	w.current.tcount += length
	w.current.receipts = receps
	w.current.txs = TxSet
	w.current.header.GasUsed = *gasU

	//整合线程的状态到worker对象










	fmt.Printf("********************	commitTransactionsConcurrently == end 	********************\n")
	fmt.Printf("ConcurrentThread --------------> %+v\n", ConcurrentThread)



	/*for {

		// If we don't have enough gas for any further transactions then we're done
		if w.current.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", w.current.gasPool, "want", params.TxGas)
			break
		}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(w.current.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.chainConfig.IsEIP155(w.current.header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.chainConfig.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

		fmt.Printf("commitTransactions => %+v\n", tx)

		logs, err := w.commitTransaction(tx, coinbase)
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			w.current.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}*/

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



// 邻接表
type Vertex struct {
	Key      	string
	Parents  	[]*Vertex
	Children 	[]*Vertex
	Value    	*ConcurrentTransaction
	IsDone		bool
}

type DAG struct {
	Vertexes []*Vertex
}

func (dag *DAG) AddVertex(v *Vertex) {
	dag.Vertexes = append(dag.Vertexes, v)
}

func (dag *DAG) AddEdge(from, to *Vertex) {
	from.Children = append(from.Children, to)

	to.Parents = append(to.Parents, from)
}

func (dag *DAG) BFS(rootSlice []*Vertex) {
	q := queue.New()

	visitMap := make(map[string]bool)
	for _, v := range rootSlice {
		visitMap[v.Key] = true
		q.Add(v)
	}
	//visitMap[root.Key] = true

	//q.Add(root)

	for {
		if q.Length() == 0 {
			//fmt.Println("done")
			break
		}
		current := q.Remove().(*Vertex)

		fmt.Println("bfs key", current.Key)

		for _, v := range current.Children {
			fmt.Printf("from:%v to:%s\n", current.Key, v.Key)
			/*if v.Key == root.Key {
				panic("back root")
			}*/
			if _, ok := visitMap[v.Key]; !ok {
				visitMap[v.Key] = true
				//fmt.Println("add visit", v.Key)

				q.Add(v)
			}
		}
	}

}

func (cTxExecutor *ConcurrentTxExecutor) BFSRangeTxs(rootSlice []*Vertex, dag *DAG) {
	q := queue.New()

	visitMap := make(map[string]bool)
	for _, v := range rootSlice {
		visitMap[v.Key] = true
		q.Add(v)
	}
	//visitMap[root.Key] = true

	//q.Add(root)

	for {
		if q.Length() == 0 {
			//fmt.Println("done")
			break
		}
		//current := q.Remove().(*Vertex)
		var current *Vertex

		//fmt.Println("bfs key", current.Key)

		for {
			temp := q.Remove().(*Vertex)
			//
			if cTxExecutor.ExecuteTx(temp) == true {
				current = temp
				break
			} else {
				q.Add(temp)
			}
		}



		for _, v := range current.Children {
			//fmt.Printf("from:%v to:%s\n", current.Key, v.Key)

			if _, ok := visitMap[v.Key]; !ok {
				visitMap[v.Key] = true
				q.Add(v)
			}
		}
	}

}



const (
	NoReadOrWrite = 0
	Read = 1
	Write = 2
	ReadAndWrite = 3
)

type AddrVar struct {
	Name string
	Key string
	ReadOrWrite	uint	//read => true	write => false
	LastAccessTx	*Vertex

	LastRW			uint
	LastWriteTx		*Vertex
	LastReadTxs		[]*Vertex
}

//线程处理事务入口
/*
@名称	ConcurrentProcessTxs
@描述	线程处理相应的事务
*/
func (cTxExecutor *ConcurrentTxExecutor) ConcurrentProcessTxs(ctxs []*ConcurrentTransaction) {
	runtime.LockOSThread()		//绑定CPU线程
	defer waitGroutp.Done()			//用于等待所有线程执行完成

	//1.构造DAG，检测r-w w-w w-w冲突
	dag := &DAG{}
	AddrVarMap := make(map[common.Address][]*AddrVar)	//保存每个变量最近被访问的情况，标识一个变量： （地址，变量名，键）	三个都匹配才命中变量
	var RootSlice []*Vertex	//记录图的初始入度为0的顶点

	//遍历每一个tx
	for i, ctx := range ctxs {
		//构造顶点
		v := &Vertex{Key: strconv.Itoa(i),
			Value: ctx,
			Parents: make([]*Vertex, 0),
			Children: make([]*Vertex, 0),
		}

		//CallTx
		if v.Value.MainOrCall == CallTransaction {
			TxRWFlag := IsReadOrWrite(ctx)	//CallTx读还是写
			//fmt.Println(TxRWFlag)
			HitFlag := false	//有没有找到冲突访问的变量

			if _, ok := AddrVarMap[ctx.CallTx.CallAddr]; ok {	//之前是否已经有合约访问过目标合约地址
				//addrvar =>此合约的每一个被访问的变量
				for _, addrvar := range AddrVarMap[ctx.CallTx.CallAddr] {
					if TxRWFlag == Read {
						if _, ok := ctx.CallTx.ReadSet[addrvar.Name]; ok {	//是否匹配到对应变量
							if ctx.CallTx.ReadSet[addrvar.Name] == addrvar.Key {	//是否匹配到对应变量的对应Key

								if addrvar.LastWriteTx != nil {		//存在w-r冲突
									//fmt.Println("存在w-r冲突，构造边")
									dag.AddEdge(addrvar.LastWriteTx, v)
								} else {
									RootSlice = append(RootSlice, v)
								}
								addrvar.LastReadTxs = append(addrvar.LastReadTxs, v)

								HitFlag = true	//命中
								addrvar.LastRW = Read
								break
							}
						}
					}
					if TxRWFlag == Write {
						if _, ok := ctx.CallTx.WriteSet[addrvar.Name]; ok { //是否匹配到对应变量
							if ctx.CallTx.WriteSet[addrvar.Name] == addrvar.Key {	//是否匹配到对应变量的对应Key

								if addrvar.LastRW == Write {		//存在w-w冲突
									//fmt.Println("存在w-w冲突，构造边")
									dag.AddEdge(addrvar.LastWriteTx, v)
									addrvar.LastWriteTx = v
								} else if addrvar.LastRW == Read {		//存在r-w冲突
									//fmt.Println("存在r-w冲突，构造边")
									for _, vet := range addrvar.LastReadTxs {
										dag.AddEdge(vet, v)

									}
									addrvar.LastWriteTx = v
									addrvar.LastReadTxs = addrvar.LastReadTxs[0:0]
								}

								HitFlag = true	//命中
								addrvar.LastRW = Write
								break
							}
						}
					}
				}
			}

			/*if _, ok := AddrVarMap[ctx.CallTx.CallAddr]; ok {	//之前是否已经有合约访问过目标合约地址
				//addrvar =>此合约的每一个被访问的变量
				for _, addrvar := range AddrVarMap[ctx.CallTx.CallAddr] {
					if TxRWFlag == Read {
						if _, ok := ctx.CallTx.ReadSet[addrvar.Name]; ok {	//是否匹配到对应变量
							if ctx.CallTx.ReadSet[addrvar.Name] == addrvar.Key {	//是否匹配到对应变量的对应Key
								if addrvar.ReadOrWrite == Read {
									HitFlag = true	//命中
									//fmt.Println("read-read冲突，不构造边")
									//更新此变量访问情况
									addrvar.ReadOrWrite = TxRWFlag
									addrvar.LastAccessTx = v
									RootSlice = append(RootSlice, v)	//新增一个入度为0顶点
									break
								} else if addrvar.ReadOrWrite == Write {
									HitFlag = true	//命中
									//fmt.Println("write-read冲突，构造边")
									dag.AddEdge(addrvar.LastAccessTx, v)
									//更新此变量访问情况
									addrvar.ReadOrWrite = TxRWFlag
									addrvar.LastAccessTx = v
									break
								}
							}
						}
					}
					if TxRWFlag == Write {
						if _, ok := ctx.CallTx.WriteSet[addrvar.Name]; ok { //是否匹配到对应变量
							if ctx.CallTx.WriteSet[addrvar.Name] == addrvar.Key {	//是否匹配到对应变量的对应Key
								HitFlag = true	//命中
								//fmt.Println("read-write或 w-w冲突，构造边")
								dag.AddEdge(addrvar.LastAccessTx, v)
								//更新此变量访问情况
								addrvar.ReadOrWrite = TxRWFlag
								addrvar.LastAccessTx = v 	//write-read或 w-w冲突，构造边
								break
							}
						}
					}
				}
			}*/


			if HitFlag == false {		//Tx没有匹配到读写记录,新建访问变量，目前不支持同时读写变量的Tx
				//fmt.Println("未命中")
				RWSet := v.Value.CallTx.ReadSet
				if TxRWFlag == Read {
					RWSet = v.Value.CallTx.ReadSet
				} else if TxRWFlag == Write {
					RWSet = v.Value.CallTx.WriteSet
				}
				for varname, key := range RWSet {
					addv := &AddrVar{
						Name: varname,
						Key: key,
						ReadOrWrite: TxRWFlag,
						LastRW: TxRWFlag,
						LastAccessTx: v,
						LastReadTxs: make([]*Vertex, 0),
					}
					if TxRWFlag == Read {
						addv.LastReadTxs = append(addv.LastReadTxs, v)
					} else if TxRWFlag == Write {
						addv.LastWriteTx = v
					}

					AddrVarMap[ctx.CallTx.CallAddr] = append(AddrVarMap[ctx.CallTx.CallAddr], addv)
					RootSlice = append(RootSlice, v)
				}
			}
		}


		//MainTx
		//对于MainTx，只匹配合约地址，目前不考虑MainTx被调用的情况
		if v.Value.MainOrCall == MainTransaction {
			/*txdata, _, _, _ := types.GetInformation(ctx.Transaction)
			if _, ok := AddrVarMap[*(txdata.Recipient)]; ok { //之前是否已经有合约访问过目标合约地址
				dag.AddEdge(AddrVarMap[*(txdata.Recipient)][0].LastAccessTx, v)
				AddrVarMap[*(txdata.Recipient)][0].LastAccessTx = v
			} else {
				addv := &AddrVar{
					LastAccessTx: v,
				}
				AddrVarMap[*(txdata.Recipient)] = append(AddrVarMap[*(txdata.Recipient)], addv)
				RootSlice = append(RootSlice, v)
			}*/
			RootSlice = append(RootSlice, v)
		}
	}


	/*v1 := &Vertex{Key: "1"}



	v1 := &Vertex{Key: "1"}
	v2 := &Vertex{Key: "2"}
	v3 := &Vertex{Key: "3"}
	v4 := &Vertex{Key: "4"}
	v5 := &Vertex{Key: "5"}

	// dag
	//     5
	//   >
	//  /
	// 1----->2
	//  \   >   \
	//   > /     >
	//   3-------->4

	dag.AddEdge(v1, v5)
	dag.AddEdge(v1, v2)
	//dag.AddEdge(v2, v1)
	dag.AddEdge(v1, v3)
	dag.AddEdge(v3, v4)
	dag.AddEdge(v3, v2)
	dag.AddEdge(v2, v4)*/


	//按照BFS规则遍历树中每一个顶点，输入：DAG和图中所有入度为0的顶点
	cTxExecutor.BFSRangeTxs(RootSlice, dag)

}

func IsReadOrWrite(ctx *ConcurrentTransaction) uint {
	if len(ctx.CallTx.ReadSet) != 0 {
		return Read
	}
	if len(ctx.CallTx.WriteSet) != 0 {
		return Write
	}
	if len(ctx.CallTx.ReadSet) != 0 && len(ctx.CallTx.WriteSet) != 0 {
		return ReadAndWrite
	}
	return NoReadOrWrite
}

//尝试检查TX 是否可执行，若执行成功且Tx 状态为Completed，返回ture, ture表示该TX已完成执行，但不保证已提交
func (cTxExecutor *ConcurrentTxExecutor) ExecuteTx(vertex *Vertex) bool {

	if vertex.Value.MainOrCall == CallTransaction {
		ctx := vertex.Value

		//1.检查父节点对应Tx是否complete
		for _, parent := range vertex.Parents {
			if len(vertex.Parents) != 0 && parent.Value.CallTx.TxState != Completed && ctx.CallTx.PointerOfMainTx != parent.Value.CallTx.PointerOfMainTx {
				return false
			}
		}
		//2.检查数据依赖是否满足
		if ctx.CallTx.DataRVP !=0 {
			return false
		}
		//3.检查是否达到提交点
		if ctx.CallTx.CanAbort == false {
			if ctx.CallTx.CommitFlag == true {
				//可以执行
				ApplyCTxForCallTx(cTxExecutor.chainConfig, cTxExecutor.chain, &cTxExecutor.coinbase, cTxExecutor.gasPool, cTxExecutor.State, cTxExecutor.header, vertex.Value, &cTxExecutor.GasUsed, *cTxExecutor.chain.GetVMConfig(), ctx.CallTx.PointerOfMainTx.MainTx.EVMState.CallType)
				ctx.CallTx.TxState = Completed
				//ctx.CallTx.PointerOfMainTx.MainTx.MainRTArgs.ReturnData = res
				ctx.CallTx.PointerOfMainTx.MainTx.CallReturnSignal = true	//MainTx can go on
				//fmt.Printf("CallTx complete CanAbort  %+v\n", ctx)
				return true
			} else {
				return false
			}
		} else if ctx.CallTx.CanAbort == true {
			//可以执行
			ApplyCTxForCallTx(cTxExecutor.chainConfig, cTxExecutor.chain, &cTxExecutor.coinbase, cTxExecutor.gasPool, cTxExecutor.State, cTxExecutor.header, vertex.Value, &cTxExecutor.GasUsed, *cTxExecutor.chain.GetVMConfig(), ctx.CallTx.PointerOfMainTx.MainTx.EVMState.CallType)
			ctx.CallTx.TxState = Executed
			//ctx.CallTx.PointerOfMainTx.MainTx.MainRTArgs.ReturnData = res
			ctx.CallTx.PointerOfMainTx.MainTx.CallReturnSignal = true	//MainTx can go on
			//fmt.Printf("CallTx complete CanNotAbort  %+v\n", ctx)
			return true
		}
		
	} else if vertex.Value.MainOrCall == MainTransaction {
		ctx := vertex.Value

		if ctx.MainTx.TxState == UnCompleted && ctx.MainTx.CallReturnSignal == true {

			//执行
			receipt, err :=  ApplyCTxForMainTx(cTxExecutor.chainConfig, cTxExecutor.chain, &cTxExecutor.coinbase, cTxExecutor.gasPool, cTxExecutor.State, cTxExecutor.header, vertex.Value, ctx.MainTx.UsedGas, *cTxExecutor.chain.GetVMConfig())
			if err != nil {
				return false
			}

			//因调用行为导致的挂起
			if ctx.MainTx.EVMState.PendingFlag == true {
				ctx.MainTx.CallReturnSignal = false
				ctx.MainTx.EVMState.PendingFlag = false


				ctx.MainTx.PointerOfCallTxs[ctx.MainTx.CurrentSegment].CallTx.DataRVP = 0

				ctx.MainTx.CurrentSegment += 1
				if ctx.MainTx.CurrentSegment > ctx.MainTx.CommitPoint {
					for _, callTx := range ctx.MainTx.PointerOfCallTxs {
						if callTx.CallTx.CanAbort == true && callTx.CallTx.TxState == Executed {
							callTx.CallTx.TxState = Completed
							//callTx.CallTx.CommitFlag = true
						} else {
							callTx.CallTx.CommitFlag = true
						}
					}
				}
			}

			//交易完成执行，修改交易状态
			if ctx.MainTx.EVMState.Halts == true && ctx.MainTx.EVMState.PendingFlag == false {
				ctx.MainTx.TxState = Completed
				cTxExecutor.receipts[ctx.TxIndex] = receipt	//暂时，写入区块需要注意顺序
				ctx.MainTx.CurrentSegment += 1
				if ctx.MainTx.CurrentSegment > ctx.MainTx.CommitPoint {
					for _, callTx := range ctx.MainTx.PointerOfCallTxs {
						if callTx.CallTx.CanAbort == true && callTx.CallTx.TxState == Executed {
							callTx.CallTx.TxState = Completed
							//callTx.CallTx.CommitFlag = true
						} else {
							callTx.CallTx.CommitFlag = true
						}
					}
				}				

				//fmt.Printf("receipts  %+v\n", receipt)
				//fmt.Printf("MainTx complete  %+v\n", ctx)
				return true
			}


		} else {
			return false
		}



	}

	return false


}

//给定CallTx，初始化EVM返回执行结果
func ApplyCTxForCallTx(config *params.ChainConfig, bc core.ChainContext, author *common.Address, gp *core.GasPool, statedb *state.StateDB, header *types.Header, ctx *ConcurrentTransaction, usedGas *uint64, cfg vm.Config, CallType uint) ([]byte, error) {
	tx := ctx.Transaction
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	types.SetMsgData(&msg, ctx.CallTx.InputData)	//把输入数据修改为MainTx提供的数据
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	context := core.NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, statedb, config, cfg)
	// Apply the transaction to the current state (included in the env)

	//sender := vm.AccountRef(msg.From())
	vmenv.MainOrCall = false

	var res []byte
	var returnGas uint64

	if CallType == 1 {
		//Call
		res, returnGas, err = vmenv.Call(ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Contract, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.ToAddr, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Args, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Gas, &ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Value )
	} else if CallType == 2 {
		//StaticCall
		res, returnGas, err = vmenv.StaticCall(ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Contract, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.ToAddr, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Args, ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Gas)
	}


	ctx.CallTx.PointerOfMainTx.MainTx.EVMState.ReturnData = res
	ctx.CallTx.PointerOfMainTx.MainTx.EVMState.ReturnGas = returnGas
	ctx.CallTx.PointerOfMainTx.MainTx.EVMState.Err = err

	//result, err := core.ApplyMessage(vmenv, msg, gp)
	//*usedGas += result.UsedGas

	return nil, nil
}


//执行MainTx
func ApplyCTxForMainTx(config *params.ChainConfig, bc core.ChainContext, author *common.Address, gp *core.GasPool, statedb *state.StateDB, header *types.Header, ctx *ConcurrentTransaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {


	//如果不是第一次执行，恢复执行环境
	if ctx.MainTx.EVMState.PC != 0 {
		EVMSt := ctx.MainTx.EVMState

		EVMSt.MainOrCall = true		//MainTx调用
		EVMSt.CallReturnFlag = true	//调用返回的恢复

		//vmenv.Call(vm.AccountRef(msg.From()), *msg.To(), msg.Data(), msg.Gas(), msg.Value())

		BeforeGas := EVMSt.Contract.Gas
		ret, Gas, err := vm.ReRun(EVMSt, EVMSt.Contract, ctx.Transaction.Data(), false)
		*usedGas += BeforeGas - Gas
		ctx.MainTx.EVMState = EVMSt

		//结束调用
		if EVMSt.Halts == true {
			txdata, _, _, _ := types.GetInformation(ctx.Transaction)
			result := &core.ExecutionResult{
				UsedGas:    txdata.GasLimit - Gas,
				Err:        err,
				ReturnData: ret,
			}

			var root []byte
			tx := ctx.Transaction

			// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
			// based on the eip phase, we're passing whether the root touch-delete accounts.
			receipt := types.NewReceipt(root, result.Failed(), 0)
			receipt.TxHash = tx.Hash()
			receipt.GasUsed = result.UsedGas
			//receipt.GasUsed = 1111
			// if the transaction created a contract, store the creation address in the receipt.
			if tx.To() == nil {
				receipt.ContractAddress = crypto.CreateAddress(EVMSt.Context.Origin, tx.Nonce())
			}
			// Set the receipt logs and create a bloom for filtering
			receipt.Logs = statedb.GetLogs(tx.Hash())
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
			receipt.BlockHash = statedb.BlockHash()
			receipt.BlockNumber = header.Number
			receipt.TransactionIndex = ctx.TxIndex

			return receipt, err
		}

		return nil, nil

	}


	//第一次MainTx调用
	tx := ctx.Transaction
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	context := core.NewEVMContext(msg, header, bc, author)
	// Create a new environment which holds all relevant information
	// about the transaction and calling mechanisms.
	vmenv := vm.NewEVM(context, statedb, config, cfg)

	// Apply the transaction to the current state (included in the env)
	result, err := core.ApplyMessage(vmenv, msg, gp)
	if err != nil {
		return nil, err
	}
	//执行以后保存EVM执行状态
	ctx.MainTx.EVMState = vmenv

	*usedGas += result.UsedGas

	//没有调用行为，第一次就返回结果
	if vmenv.Halts == true {
		var root []byte
		// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
		// based on the eip phase, we're passing whether the root touch-delete accounts.
		receipt := types.NewReceipt(root, result.Failed(), 0)
		receipt.TxHash = tx.Hash()
		receipt.GasUsed = result.UsedGas
		// if the transaction created a contract, store the creation address in the receipt.
		if msg.To() == nil {
			receipt.ContractAddress = crypto.CreateAddress(vmenv.Context.Origin, tx.Nonce())
		}
		// Set the receipt logs and create a bloom for filtering
		receipt.Logs = statedb.GetLogs(tx.Hash())
		receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		receipt.BlockHash = statedb.BlockHash()
		receipt.BlockNumber = header.Number
		receipt.TransactionIndex = ctx.TxIndex

		return receipt, err
	}

	//如果存在调用行为，先返回空结果
	return nil, err
}
