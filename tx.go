package tx4go

import (
	"context"
	"github.com/google/uuid"
	"sync"
)

// --------------------------------------------------------------------------------
type txStatus int

const (
	txStatusPending  txStatus = iota + 1000 // 刚刚创建的 tx
	txStatusCommit                          // 已经提交的 tx
	txStatusRollback                        // 已经回滚的 tx
)

// --------------------------------------------------------------------------------
type txType int

const (
	txTypeRoot   txType = iota + 1000 // 主事务
	txTypeBranch                      // 分支事务
)

// --------------------------------------------------------------------------------
// TxInfo 事务基本信息
type TxInfo struct {
	TxId       string `json:"tx_id"`       // 事务 id
	ServerName string `json:"server_name"` // 事务服务名称
	ServerAddr string `json:"server_addr"` // 事务服务地址
	ServerUUID string `json:"server_uuid"` // 事务服务uuid
}

// --------------------------------------------------------------------------------
type Tx struct {
	id string
	mu sync.Mutex
	w  sync.WaitGroup

	txInfo         *TxInfo // 当前事务的信息
	rootTxInfo     *TxInfo // 主事务的信息
	tType          txType
	status         txStatus       // 用于主事务端维护各分支事务的状态
	txList         map[string]*Tx // 用于主事务端维护各分支事务
	ctx            context.Context
	confirmHandler func()
	cancelHandler  func()

	isConfirm bool
	isCancel  bool
}

func Begin(ctx context.Context, confirm func(), cancel func()) (*Tx, error) {
	var t = &Tx{}
	t.id = uuid.New().String()
	t.ctx = ctx

	t.confirmHandler = confirm
	t.cancelHandler = cancel

	var rootTxInfo *TxInfo
	if t.ctx != nil {
		rootTxInfo = txInfoWithContext(t.ctx)
	}

	// 构建当前事务的信息
	t.txInfo = &TxInfo{}
	t.txInfo.TxId = t.id
	t.txInfo.ServerName = m.serverName
	t.txInfo.ServerAddr = m.serverAddr
	t.txInfo.ServerUUID = m.serverUUID

	if rootTxInfo == nil {
		// 如果 rootTxInfo 为空，则表示当前事务为主事务
		t.tType = txTypeRoot

		// 将当前事务的信息放置到 ctx 中
		t.ctx = txInfoToContext(ctx, t.txInfo)
	} else {
		// 如果 rootTxInfo 不为空，则表示当前事务为分支事务
		t.tType = txTypeBranch

		// 构建当前事务的主事务信息
		t.rootTxInfo = rootTxInfo

		// 发消息告知主事务，有分支事务建立
		if err := t.register(); err != nil {
			return nil, err
		}
	}

	// 添加事务到管理器中
	m.addTx(t)

	return t, nil
}

func (this *Tx) Context() context.Context {
	return this.ctx
}

// --------------------------------------------------------------------------------
// register 分支事务向主事务注册（分）
func (this *Tx) register() error {
	return m.registerTx(this.rootTxInfo, this.txInfo)
}

// registerTx 分支事务向主事务发起注册消息之后，主事务添加分支事务信息（主）
func (this *Tx) registerTx(tx *Tx) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if tx == nil {
		return
	}

	if this.txList == nil {
		this.txList = make(map[string]*Tx)
	}
	this.txList[tx.id] = tx

	this.w.Add(1)
}

// --------------------------------------------------------------------------------
// Commit 提交事务
// 分支事务 - 发消息告知主事务，将该分支事务的状态调整为提交状态
// 主事务 - 等待所有分支事务的消息，并判断所有分支事务的状态，决定是 cancel 还是 confirm，向所有的分支事务派发对应的消息
func (this *Tx) Commit() (err error) {
	if this.tType == txTypeBranch {
		// 如果是分支事务，则向主事务发送消息
		return m.commitTx(this.rootTxInfo, this.txInfo)
	}

	if this.isCancel == true || this.isConfirm == true {
		return
	}

	// 等待所有的子事务操作完成
	this.w.Wait()

	// 检查子事务的状态，如果有状态不为 commit 的，则进行 cancel 操作，否则进行 confirm 操作
	var shouldCancel = false
	for _, tx := range this.txList {
		if tx.status != txStatusCommit {
			shouldCancel = true
			break
		}
	}

	if shouldCancel {
		// 通知所有的分支事务，进行 cancel 操作
		for _, tx := range this.txList {
			m.cancelTx(tx.txInfo, this.txInfo)
		}
		this.cancelTx()
	} else {
		// 通知所有的分支事务，进行 confirm 操作
		for _, tx := range this.txList {
			m.confirmTx(tx.txInfo, this.txInfo)
		}
		this.confirmTx()
	}

	return nil
}

// commitTx 分支事务提交之后，主事务将其维护的分支事务的状态标记为提交（主）
func (this *Tx) commitTx(txId string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	var tx = this.txList[txId]
	if tx != nil && tx.status == txStatusPending {
		tx.status = txStatusCommit
		this.w.Done()
	}
}

// cancelTx 取消事务（主、分）
func (this *Tx) cancelTx() {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.cancelHandler != nil && this.isCancel == false {
		this.cancelHandler()
	}

	this.isCancel = true

	m.delTx(this.id)
}

// confirmTx 确认事务（主、分）
func (this *Tx) confirmTx() {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.confirmHandler != nil && this.isConfirm == false {
		this.confirmHandler()
	}

	this.isConfirm = true

	m.delTx(this.id)
}

// --------------------------------------------------------------------------------
// Rollback 回滚事务
// 分支事务 - 发消息告知主事务，将该分支事务的状态调整为回滚状态
// 主事务 - 等待所有分支事务的消息，接收到所有分支事务的消息之后，向所有的分支事务派发 cancel 消息
func (this *Tx) Rollback() (err error) {
	if this.tType == txTypeBranch {
		// 如果是分支事务，则向主事务发送消息并直接将当前事务标记为已取消
		this.isCancel = true
		err = m.rollbackTx(this.rootTxInfo, this.txInfo)
	} else {
		if this.isCancel == true || this.isConfirm == true {
			return
		}
		this.isCancel = true

		// 等待所有的子事务操作完成
		this.w.Wait()

		// 通知所有的分支事务，进行 cancel 操作
		for _, tx := range this.txList {
			// 只向已提交的分支事务发送 cancel 消息
			if tx.status == txStatusCommit {
				m.cancelTx(tx.txInfo, this.txInfo)
			}
		}
	}
	this.cancelTx()

	return err
}

// rollbackTx 分支事务回滚之后，主事务将其维护的分支事务的状态标记为回滚（主）
func (this *Tx) rollbackTx(txId string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	var tx = this.txList[txId]
	if tx != nil && tx.status == txStatusPending {
		tx.status = txStatusRollback
		this.w.Done()
	}
}
