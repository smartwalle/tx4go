package tx4go

import "sync"

type txHub struct {
	mu     sync.Mutex
	txList map[string]*Tx
}

func newTxHub() *txHub {
	var h = &txHub{}
	h.txList = make(map[string]*Tx)
	return h
}

func (this *txHub) addTx(tx *Tx) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if tx != nil {
		this.txList[tx.id] = tx
	}
}

func (this *txHub) delTx(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.txList, id)
}

func (this *txHub) getTx(id string) *Tx {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.txList[id]
}

func (this *txHub) getTxList() []*Tx {
	this.mu.Lock()
	defer this.mu.Unlock()

	var list = make([]*Tx, 0, len(this.txList))
	for _, tx := range this.txList {
		list = append(list, tx)
	}

	return list
}
