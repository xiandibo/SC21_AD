package core

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/occmanager"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/queue"
	"runtime"
	"sync"
)

//交易执行状态
const (
	UnExecuted = 0	//未执行
	CanCommit = 1	//已执行未提交
	Abort = 2	//已执行已提交

	ConcurrentThread = 20
)

type environment struct {

	state *state.StateDB
	tcount    int
	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt
	gasPool   *GasPool

	Lock	sync.Mutex	//用于并发写入txs和receipts
}

type OCCWorker struct {

	chainConfig *params.ChainConfig
	chain       *BlockChain
	current *environment
	coinbase common.Address
	vmcfg vm.Config
}

type ConcurrentExecutor struct {
	CurrentBlockWorker	*OCCWorker
	GasPool	*GasPool
	LocalTempState	*state.StateDB
	OCCGlobalManager *occmanager.OCCGlobalManager

	receipts []*types.Receipt

	Dag *DAG
}

type DAG struct {
	Vertexes []*types.Transaction
	LatestAccessTx map[[16]byte]uint
	//Roots []*types.Transaction
	Que	*queue.Queue
	visitMap map[uint]bool
	executedMap map[uint]bool
	pendingTxCount uint

	Mu sync.RWMutex
}


var waitGroutp = sync.WaitGroup{}



func ConcurValidate(TxSet types.Transactions, coinbase *common.Address, statedb *state.StateDB, cfg vm.Config, config *params.ChainConfig, bc *BlockChain, header *types.Header, usedGas *uint64) (types.Receipts, error) {

	fmt.Printf("********************	OCCValidateConcurrently == start 	********************\n")
	//start := time.Now()
	dag := &DAG{
		LatestAccessTx: make(map[[16]byte]uint),
	}

	//var TxForThreads []types.Transactions
	var ContractCreation [] *types.Transaction

	w := &OCCWorker{
		chainConfig: config,
		current: &environment{
			state:statedb,
		},
		chain: bc,
		//coinbase: *coinbase,
		vmcfg: cfg,
	}

	w.current.header = header
	w.current.gasPool = new(GasPool).AddGas(w.current.header.GasLimit)

	//分离出合约创建tx
	for i := 0; i < len(TxSet); i++ {
		if TxSet[i].To() == nil {
			ContractCreation = append(ContractCreation, TxSet[i])
			TxSet = append(TxSet[:i], TxSet[i+1:]...)
			i = i -1
		}
	}

	length := len(TxSet)
	var receps = make([]*types.Receipt, length)

	for _, tx := range ContractCreation {
		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)
		receipt, _ := ApplyTransaction(w.chainConfig, w.chain, nil, w.current.gasPool, w.current.state, w.current.header, tx, &w.current.header.GasUsed, *w.chain.GetVMConfig())
		//w.current.txs = append(w.current.txs, tx)
		receps = append(receps, receipt)
		w.current.tcount++
	}

	if len(TxSet) != 0 {

		/*for _, tx := range TxSet {
			fmt.Println(tx.GraphInfor)
		}*/

		dag.Vertexes = TxSet
		var RootSlice []*types.Transaction

		for _, tx := range TxSet {
			if len(tx.GraphInfor.ParentNodeIndex) == 0 {
				RootSlice = append(RootSlice, tx)
			}
		}
		//dag.Roots = RootSlice
		visitMap := make(map[uint]bool)
		executedMap := make(map[uint]bool)
		q := queue.New()
		for _, v := range RootSlice {
			visitMap[v.GraphInfor.Key] = true
			q.Add(v)
		}
		dag.Que = q
		dag.visitMap = visitMap
		dag.executedMap = executedMap

		waitGroutp.Add(ConcurrentThread)

		for i := 0; i < ConcurrentThread; i++ {
			gasP := new(GasPool).AddGas(uint64(99999999))
			//parent := w.chain.CurrentBlock()
			//stateThread, _ := w.chain.StateAt(parent.Root())
			stateThread := w.current.state.Copy()
			ConcurrentExe := &ConcurrentExecutor{
				CurrentBlockWorker: w,
				GasPool:            gasP,
				LocalTempState:     stateThread,

				Dag:      dag,
				receipts: receps,
			}
			go ConcurrentExe.ConcurrentExecuteTxs()
		}

		waitGroutp.Wait()

	}

	//fmt.Println(common.PrettyDuration(time.Since(start)))
	fmt.Printf("********************	OCCValidateConcurrently == end 	********************\n")
	fmt.Printf("ConcurrentThread --------------> %+v\n", ConcurrentThread)

	*usedGas = 0
	/*for _, re := range receps {
		fmt.Println(*re)
	}*/

	return receps, nil
}

func (d *DAG) GetTx() (*types.Transaction, uint) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	if d.Que.Length() == 0 {
		return nil, d.pendingTxCount
	} else {
		tx := d.Que.Remove().(*types.Transaction)
		d.pendingTxCount++
		return tx, d.pendingTxCount
	}

}

func (d *DAG) TxFinished(tx *types.Transaction) {
	d.Mu.Lock()
	defer d.Mu.Unlock()

	d.executedMap[tx.GraphInfor.Key] = true
	d.pendingTxCount--

	for _, v := range tx.GraphInfor.ChildNodeIndex {
		tx := d.Vertexes[v]
		parentvisitedflag := true
		for _, i := range tx.GraphInfor.ParentNodeIndex {
			if _, ok := d.executedMap[i]; !ok {
				parentvisitedflag = false
			}
		}
		if _, ok := d.visitMap[v]; !ok {
			if parentvisitedflag == true {
				d.visitMap[v] = true
				d.Que.Add(d.Vertexes[v])
			}
		}
	}


}

func (d *DAG) CanExecute(tx *types.Transaction) bool {
	d.Mu.RLock()
	defer d.Mu.RUnlock()

	//d.pendingTxCount--
	if tx.GraphInfor.ParentNodeIndex != nil && len(tx.GraphInfor.ParentNodeIndex) != 0 {
		for _, v := range tx.GraphInfor.ParentNodeIndex {
			if _, ok := d.executedMap[v]; ok {
				if d.executedMap[v] == false {
					return false
				}
			} else {
				return false
			}
		}
	} else {
		return true
	}

	return true
	//return true

}

func (c *ConcurrentExecutor) ConcurrentExecuteTxs() {
	runtime.LockOSThread()
	defer waitGroutp.Done()
	//var ValidatePhaseTx [] types.Transaction

	dag := c.Dag

	for  {
		//fmt.Println("***********ConcurrentExecuteTxs***********")
		//start := time.Now()
		tx, pendingTxCount := dag.GetTx()

		/*if tx != nil {
			fmt.Println(tx.GraphInfor)
		}*/




		if tx != nil {

			//fmt.Println("***********CanExecute***********8")
			//start1 := time.Now()
			for {
				if c.Dag.CanExecute(tx) == true {
					break
				}
			}
			//fmt.Println(common.PrettyDuration(time.Since(start1)))

			TempState := c.CurrentBlockWorker.current.state.CopyForThread()
			c.CurrentBlockWorker.current.state.Prepare(tx.Hash(), common.Hash{}, int(tx.GraphInfor.Key))
			receipt, err := ConcurrentApplyTransaction(c.CurrentBlockWorker.chainConfig, c.CurrentBlockWorker.chain, nil, c.GasPool, TempState, c.CurrentBlockWorker.current.state, c.CurrentBlockWorker.current.header, tx, &c.CurrentBlockWorker.current.header.GasUsed, *c.CurrentBlockWorker.chain.GetVMConfig(), nil)
			if err != nil {
				fmt.Println(err)
				break
				//return err
			}
			c.receipts[tx.GraphInfor.Key] = receipt
			c.CurrentBlockWorker.current.state.FinalizeToGlobal(TempState)
			//c.CurrentBlockWorker.current.state.Finalise(true)
			c.Dag.TxFinished(tx)
			//fmt.Println(tx.GraphInfor)
			//fmt.Println("***********Tx finished***********8")
			//fmt.Println(common.PrettyDuration(time.Since(start)))

		} else if pendingTxCount != 0 {
			//fmt.Println("***********tx == nil, continue***********")
			//fmt.Println(common.PrettyDuration(time.Since(start)))
			continue
		} else {
			//fmt.Println("***********Finish***********")
			//fmt.Println(common.PrettyDuration(time.Since(start)))
			break
		}




	}



	//return nil
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
		fmt.Println("Add edge____________________")
		fmt.Println(fromKey)
		fmt.Println(tokey)
	}

	if !IsExitKey(to.GraphInfor.ParentNodeIndex, fromKey) && fromKey != tokey {
		to.GraphInfor.ParentNodeIndex = append(to.GraphInfor.ParentNodeIndex, fromKey)
		fmt.Println("Add edge____________________")
		fmt.Println(fromKey)
		fmt.Println(tokey)
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

func (env *environment) AddTxsAndReceipt(tx *types.Transaction, receipt *types.Receipt) uint {
	env.Lock.Lock()
	defer env.Lock.Unlock()

	env.txs = append(env.txs, tx)
	receipt.TransactionIndex = uint(len(env.txs) - 1)
	env.tcount = len(env.txs)
	if len(env.txs) == 1 {
		receipt.CumulativeGasUsed = receipt.GasUsed
		//receipt.CumulativeGasUsed = 0
	} else {
		temp := env.receipts[len(env.receipts) - 1]
		receipt.CumulativeGasUsed = temp.CumulativeGasUsed + receipt.GasUsed
	}

	env.receipts = append(env.receipts, receipt)
	env.header.GasUsed = receipt.CumulativeGasUsed

	return uint(len(env.txs) - 1)
}