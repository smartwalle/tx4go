package tx4go

import (
	"github.com/google/uuid"
)

var m *Manager

func init() {
	m = &Manager{}
	m.txList = make(map[string]*Tx)
}

type Manager struct {
	txList map[string]*Tx
}

func (this *Manager) addTx(tx *Tx) {
	this.txList[tx.id] = tx
}

func Begin(confirm func(), cancel func()) *Tx {
	var nTx = &Tx{}
	nTx.id = uuid.New().String()
	nTx.tType = TxTypeRoot
	nTx.status = TxStatusPending
	nTx.confirmHandler = confirm
	nTx.cancelHandler = cancel
	m.addTx(nTx)
	return nTx
}
