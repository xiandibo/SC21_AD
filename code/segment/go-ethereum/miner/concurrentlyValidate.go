package miner

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

type ChainContext interface {
	// Engine retrieves the chain's consensus engine.
	Engine() consensus.Engine

	// GetHeader returns the hash corresponding to their hash.
	GetHeader(common.Hash, uint64) *types.Header

	GetBlockChain() *core.BlockChain

	StateAt(root common.Hash) (*state.StateDB, error)
}

func ConcurValidate(TxSet types.Transactions, coinbase *common.Address, statedb *state.StateDB, cfg vm.Config, config *params.ChainConfig, bc ChainContext, parent *types.Header, header *types.Header, usedGas *uint64) (types.Receipts, error) {

	fmt.Printf("********************	ConcurValidate == start 	********************\n")





	var ConcurrencyTxSet []*ConcurrentTransaction

	//用于分配，把事务集分配给多个线程
	TxsForThread := make(map[uint][]*ConcurrentTransaction)	//按照线程标识的事务集合
	AddrForThread := make(map[common.Address]uint)	//记录已经分配给线程的合约地址，保证相同的地址分到同一个线程集合
	var indexOfThread uint = 0	//用于循环分配Tx给线程
	var flagAddrExit = false	//标志分配时遇到新的合约地址，分配给下一个线程

	for index, tx := range TxSet {
		txdata, _, _, _ := types.GetInformation(tx)
		concurrencyTxMain := &ConcurrentTransaction{
			MainOrCall: MainTransaction,
			Transaction: tx,
			MainTx: MainTx{
				CallReturnSignal: true,
				CommitPoint: 0,		//验证节点已经确定交易不会abort
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
		ConcurrencyTxSet = append(ConcurrencyTxSet, concurrencyTxMain)
		for addr := range AddrForThread {
			if addr == *(txdata.Recipient) {
				flagAddrExit = true
				TxsForThread[AddrForThread[*(txdata.Recipient)]] = append(TxsForThread[AddrForThread[*(txdata.Recipient)]], concurrencyTxMain)
				break
			}
		}
		if flagAddrExit == false {
			AddrForThread[*(txdata.Recipient)] = indexOfThread
			TxsForThread[indexOfThread] = append(TxsForThread[indexOfThread], concurrencyTxMain)
			indexOfThread = (indexOfThread + 1) % ConcurrentThread
		}


		for idex, CallOption := range tx.ConcurrencyInformation.CallInformation {
			canAbortFlag := uint(idex) < tx.ConcurrencyInformation.CommitPoint
			concurrencyTxCall := &ConcurrentTransaction{
				MainOrCall: CallTransaction,
				Transaction: tx,
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
			ConcurrencyTxSet = append(ConcurrencyTxSet, concurrencyTxCall)
			concurrencyTxMain.MainTx.PointerOfCallTxs = append(concurrencyTxMain.MainTx.PointerOfCallTxs, concurrencyTxCall)

			for addr := range AddrForThread {
				if addr == concurrencyTxCall.CallTx.CallAddr {
					flagAddrExit = true
					TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]] = append(TxsForThread[AddrForThread[concurrencyTxCall.CallTx.CallAddr]], concurrencyTxCall)
					break
				}
			}
			if flagAddrExit == false {
				AddrForThread[concurrencyTxCall.CallTx.CallAddr] = indexOfThread
				TxsForThread[indexOfThread] = append(TxsForThread[indexOfThread], concurrencyTxCall)
				indexOfThread = (indexOfThread + 1) % ConcurrentThread
			}
		}

	}

	for threadInd, txs := range TxsForThread{
		fmt.Printf("threadInd ======> %+v\n", threadInd)
		for _, ctx := range txs{
			fmt.Printf("ConcurrencyTxSet -------------->\n %+v\n", ctx)
		}
	}

	//fmt.Printf("%+v\n", runtime.NumCPU())
	//fmt.Printf("%+v\n", runtime.NumGoroutine())

	var ExecutorSet []*ConcurrentTxExecutor

	for i := 0; i < ConcurrentThread; i++ {
		//对每一个线程执行器，都初始化相同的state，目的是在最后汇总 stateObjects
		gasP := new(core.GasPool).AddGas(uint64(99999999))
		state, _ := bc.StateAt(parent.Root)
		envCon := &ConcurrentTxExecutor{
			chainConfig: config,
			chain: bc.GetBlockChain(),
			tcount: 0,
			GasUsed:0,
			//gasPool: w.current.gasPool,
			gasPool: gasP,
			header: header,
			coinbase: *coinbase,
			State: state,
			receipts: make(map[uint]*types.Receipt),
		}
		ExecutorSet = append(ExecutorSet, envCon)

	}

	waitGroutp.Add(ConcurrentThread)
	//整合txcount和header的gasused   收据
	for i := 0; i < ConcurrentThread; i++ {
		go ExecutorSet[i].ConcurrentProcessTxs(TxsForThread[uint(i)])
	}

	//等待每一个线程结束运行
	waitGroutp.Wait()

	//finalize每一个线程，把每一个Executor的对象整合到worker对象中
	length := len(TxSet)
	//gasU := new(uint64)
	var receps = make([]*types.Receipt, length)

	//遍历每一个线程的执行结果
	for i := 0; i < ConcurrentThread; i++ {
		if ExecutorSet[i].chainConfig.IsByzantium(ExecutorSet[i].header.Number) {
			ExecutorSet[i].State.FinaliseForThreads(true)	//把dirty的stateobject添加到pending,与之前不同的是，不删除dirty的journal
		}
		statedb.AddStateToWorker(ExecutorSet[i].State)		//把线程的state添加到worker的state中
		for k, v := range ExecutorSet[i].receipts {
			receps[k] = v
			*usedGas += v.GasUsed
		}
		//*gasU += ExecutorSet[i].GasUsed
	}
	for i := 0; i < len(receps); i++ {
		if i == 0 {
			receps[i].CumulativeGasUsed = receps[i].GasUsed
			continue
		}
		receps[i].CumulativeGasUsed = receps[i-1].CumulativeGasUsed + receps[i].GasUsed
	}

	//w.current.tcount += length
	//w.current.receipts = receps
	//w.current.txs = TxSet
	//header.GasUsed = *gasU

	//整合线程的状态到worker对象







	fmt.Printf("********************	ConcurValidate == end 	********************\n")

	return receps, nil
}
