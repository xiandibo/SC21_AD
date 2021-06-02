package twoplmanager

import (
	"bytes"
	"crypto/md5"
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

const (
	ReadLock = 2	//读锁，共享锁
	WriteLock = 3	//写锁，排它锁
)

type Record struct {
	Address 		common.Address	//合约地址
	Key				common.Hash		//变量hashkey

	Lock			bool
	LockKind		uint
	LockCount		uint

	PreValue		common.Hash

	MuForLock		sync.Mutex
}

func (r *Record) SetPreValue(val common.Hash) {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	r.PreValue = val
}

func (r *Record) LockRecord(kind uint) bool {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	if r.Lock == false && r.LockCount == 0 {
		r.Lock = true
		r.LockKind = kind
		r.LockCount++
		return true
	} else {
		if r.LockKind == ReadLock && kind == ReadLock {
			r.LockCount++
			return true
		}
		//待定
		/*if r.LockKind == ReadLock && kind == WriteLock && r.LockCount == 1 {
			r.LockKind = kind
			return true
		}*/
		if r.LockKind == WriteLock && kind == WriteLock {
			return false
		}

	}

	return false
}

func (r *Record) UnLockRecord() {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	if r.Lock == false {
		return
	} else {
		if r.LockKind == ReadLock {
			r.LockCount--
			if r.LockCount == 0 {
				r.Lock = false
				r.LockKind = 0
			}
		} else if r.LockKind == WriteLock {
			r.LockCount--
			r.Lock = false
			r.LockKind = 0
		}


	}
}

func (r *Record) UpdateRecord() bool {
	r.MuForLock.Lock()
	defer r.MuForLock.Unlock()

	if r.Lock == true && r.LockKind == ReadLock && r.LockCount == 1 {
		r.LockKind = WriteLock
		return true
	} else {
		return false
	}
}

type TwoPLGlobalManger struct{
	GlobalRecords	map[[16]byte]*Record

	Mu				sync.RWMutex
}

func (g *TwoPLGlobalManger) ApplyForLock(ReadOrWrite uint, address common.Address, key common.Hash) (bool, *Record) {

	record := g.GetRecord(address,key)
	result := record.LockRecord(ReadOrWrite)

	//是否需要DEEPCOPY？
	return result, record

}

func (g *TwoPLGlobalManger) UpdateForReadLock(address common.Address, key common.Hash) (bool, *Record) {
	record := g.GetRecord(address,key)
	result := record.UpdateRecord()

	return result, record
}

func (g *TwoPLGlobalManger) GetRecord(address common.Address, key common.Hash) *Record {
	g.Mu.Lock()
	defer g.Mu.Unlock()

	temp := BytesCombine(address.Bytes(), key.Bytes())
	//g.Mu.RLock()
	if record, ok := g.GlobalRecords[md5.Sum(temp)]; ok {
		//g.Mu.RUnlock()
		return record
	} else {
		//g.Mu.RUnlock()
		record := g.AcquireNewRecord(address,key)
		return record
	}

}

//插入新记录
func (g *TwoPLGlobalManger) AcquireNewRecord(address common.Address, key common.Hash) *Record {
	//g.Mu.Lock()
	//defer g.Mu.Unlock()

	r := &Record{
		Address: address,
		Key: key,
		Lock: false,
		LockKind: 0,
	}

	temp := BytesCombine(address.Bytes(), key.Bytes())
	g.GlobalRecords[md5.Sum(temp)] = r

	return r

}

type LocalManager struct {
	ReadSet		           map[[16]byte]*Record
	WriteSet               map[[16]byte]*Record
	LockedRecord			map[[16]byte]*Record
	TwoPLGlobalMangerPointer	*TwoPLGlobalManger
}

func (l *LocalManager) IsExistReadLock(address common.Address, key common.Hash) bool {
	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.ReadSet[md5.Sum(temp)]; ok {
		return true
	} else {
		return false
	}
}

func (l *LocalManager) IsExistWriteLock(address common.Address, key common.Hash) bool {
	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.WriteSet[md5.Sum(temp)]; ok {
		return true
	} else {
		return false
	}
}

func (l *LocalManager) AddLockedRecord(address common.Address, key common.Hash, record *Record) {
	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.LockedRecord[md5.Sum(temp)]; ok {
		return
	} else {
		l.LockedRecord[md5.Sum(temp)] = record
		return
	}

}

func (l *LocalManager) AddReadSet(address common.Address, key common.Hash, record *Record) {
	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.ReadSet[md5.Sum(temp)]; ok {
		return
	} else {
		l.ReadSet[md5.Sum(temp)] = record
		return
	}

}

func (l *LocalManager) AddWriteSet(address common.Address, key common.Hash, record *Record) {
	temp := BytesCombine(address.Bytes(), key.Bytes())
	if _, ok := l.WriteSet[md5.Sum(temp)]; ok {
		return
	} else {
		l.WriteSet[md5.Sum(temp)] = record
		return
	}

}

//连接两个byte
func BytesCombine(pBytes ...[]byte) []byte {
	return bytes.Join(pBytes, []byte(""))
}