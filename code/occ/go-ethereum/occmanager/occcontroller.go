package occmanager

import (
	"bytes"
	"crypto/md5"
	"github.com/ethereum/go-ethereum/common"
	"sync"
)



//对应并发读写对象
type Record struct {
	TransactionID 	uint			//目前record的最近被修改的事物的ID

	Address 		common.Address	//合约地址
	Key				common.Hash		//变量hashkey

	//LatestVersion 	bool
	Lock			bool			//锁定Record

	MuForLock		sync.Mutex		//修改此record的并发锁
}

func (r *Record) LockRecord() bool {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	if r.Lock == true {
		return false
	} else {
		r.Lock = true
		return true
	}
}

func (r *Record) UnLockRecord() {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	r.Lock = false

}

func (r *Record) UpdateTID(TID uint) {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	r.TransactionID = TID
}


type OCCGlobalManager struct {
	CurrentTID		uint			//目前可分配的TID
	Mu				sync.RWMutex
	muTID			sync.Mutex
	GlobalRecords	map[[16]byte]*Record	//存储全局Record状态

}

//从全局状态记录中返回record
func (g *OCCGlobalManager) GetRecordFromGlobal(address common.Address, key common.Hash) *Record {
	g.Mu.Lock()
	defer g.Mu.Unlock()

	temp := BytesCombine(address.Bytes(), key.Bytes())

	//res := md5.Sum(temp)

	if record, ok := g.GlobalRecords[md5.Sum(temp)]; ok {
		r := new(Record)
		*r = *record	//deepcopy
		return r
	} else {
		record := g.AcquireNewRecord(address,key)
		r := new(Record)
		*r = *record
		return r
	}
}

func (g *OCCGlobalManager) AcquireNewRecord(address common.Address, key common.Hash) *Record {

	r := &Record{
		//TransactionID: g.CurrentTID,
		TransactionID: 0,
		Address: address,
		Key: key,
		Lock: false,
	}

	temp := BytesCombine(address.Bytes(), key.Bytes())
	g.GlobalRecords[md5.Sum(temp)] = r

	return r

}

func (g *OCCGlobalManager) ARecord( key [16]byte) *Record {
	g.Mu.Lock()
	defer g.Mu.Unlock()

	r := &Record{
		//TransactionID: g.CurrentTID,
		TransactionID: 0,
		//Address: nil,
		//Key: key,
		Lock: false,
	}

	g.GlobalRecords[key] = r

	return r

}

func (g *OCCGlobalManager) RecordIsExist(key [16]byte) bool {
	g.Mu.RLock()
	defer g.Mu.RUnlock()

	if _, ok := g.GlobalRecords[key]; ok {
		return true
	} else {
		return false
	}

}

func (g *OCCGlobalManager) AddNewRecord(key [16]byte) *Record {
	g.Mu.Lock()
	defer g.Mu.Unlock()

	r := &Record{
		//TransactionID: g.CurrentTID,
		TransactionID: 0,
		Address: common.Address{},
		Key: common.Hash{},
		Lock: false,
	}

	//temp := BytesCombine(address.Bytes(), key.Bytes())
	g.GlobalRecords[key] = r

	return r
}

func (g *OCCGlobalManager) LockRecord(key [16]byte) bool {
	g.Mu.RLock()
	defer g.Mu.RUnlock()

	/*_, ok := g.GlobalRecords[key]

	if !ok {
		g.ARecord(key)
	}

	if ok && g.GlobalRecords[key].Lock == true {
		return false
	} else if ok && g.GlobalRecords[key].Lock == false {
		g.GlobalRecords[key].Lock = true
		return true
	}*/

	record := g.GlobalRecords[key]

	return record.LockRecord()

	//return false
}

func (g *OCCGlobalManager) UnLockRecord(key [16]byte) {
	g.Mu.RLock()
	defer g.Mu.RUnlock()

	/*if _, ok := g.GlobalRecords[key]; ok {
		g.GlobalRecords[key].Lock = false
	}*/

	record := g.GlobalRecords[key]

	record.UnLockRecord()

}

func (g *OCCGlobalManager) AssignTID() uint {
	g.muTID.Lock()
	defer g.muTID.Unlock()

	g.CurrentTID++
	temp := g.CurrentTID

	return temp
}

func (g *OCCGlobalManager) UpdateTID(key [16]byte, TID uint) {
	g.Mu.RLock()
	defer g.Mu.RUnlock()

	record := g.GlobalRecords[key]
	record.UpdateTID(TID)

}


type OCCLocalManager struct {
	ReadSet			map[[16]byte]*Record
	WriteSet		map[[16]byte]*Record
	OCCGlobalManagerPointer *OCCGlobalManager
	AssignedTID		uint
}

func (l *OCCLocalManager) AddWriteRecord(address common.Address, key common.Hash) *Record {

	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.WriteSet[md5.Sum(temp)]; ok {
		return nil
	} else {
		r := &Record{
			TransactionID: 0,
			Address: address,
			Key: key,
			Lock: false,
		}
		return r
	}
}

func (l *OCCLocalManager) ReleaseWriteLock() {
	for _, r := range l.WriteSet {
		//l.OCCGlobalManagerPointer.UnLockRecord(k)
		r.UnLockRecord()
	}
}


//连接两个byte
func BytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}

