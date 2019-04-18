package tx4go

import (
	"github.com/google/uuid"
)

// --------------------------------------------------------------------------------
type TxStatus int

const (
	TxStatusPending TxStatus = iota + 1000
	TxStatusCommit
	TxStatusCancel
)

// --------------------------------------------------------------------------------
type TxType int

const (
	TxTypeRoot TxType = iota + 1000
	TxTypeBranch
)

// --------------------------------------------------------------------------------
type Tx struct {
	id             string
	pId            string
	tType          TxType
	status         TxStatus
	txList         map[string]*Tx
	confirmHandler func()
	cancelHandler  func()
}

func (this *Tx) Begin(confirm func(), cancel func()) *Tx {
	var nTx = &Tx{}
	nTx.id = uuid.New().String()
	nTx.tType = TxTypeBranch
	nTx.status = TxStatusPending
	nTx.pId = this.id
	nTx.confirmHandler = confirm
	nTx.cancelHandler = cancel
	this.addTx(nTx)
	return nTx
}

func (this *Tx) Commit() {
}

func (this *Tx) Rollback() {
	if this.tType == TxTypeRoot {
		if len(this.txList) > 0 {
			for _, tx := range this.txList {
				// 发送消息给 tx，告知其进行取消操作
				tx.Rollback()
			}
		}
	} else {
		// 发送消息给 root tx，告知其进行取消操作
	}
}

func (this *Tx) doConfirm() {
	if this.confirmHandler != nil {
		this.confirmHandler()
	}
}

func (this *Tx) doCancel() {
	// 只有 commit 了的才需要 cancel
	if this.status == TxStatusCommit && this.cancelHandler != nil {
		this.cancelHandler()
	}
	this.status = TxStatusCancel
}

func (this *Tx) addTx(tx *Tx) {
	if this.txList == nil {
		this.txList = make(map[string]*Tx)
	}
	this.txList[tx.id] = tx
}
