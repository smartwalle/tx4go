package tx4go

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

// --------------------------------------------------------------------------------
type txStatus int

const (
	txStatusPending        txStatus = iota + 1000 // 刚刚创建的 tx
	txStatusPendingConfirm                        // 待确认的 tx
	txStatusPendingCancel                         // 待取消的 tx
	txStatusConfirm                               // 确认的 tx
	txStatusCancel                                // 取消的 tx
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
	TxId       string    `json:"tx_id"`       // 事务 id
	ServerName string    `json:"server_name"` // 事务服务名称
	ServerAddr string    `json:"server_addr"` // 事务服务地址
	ServerUUID string    `json:"server_uuid"` // 事务服务uuid
	TTL        time.Time `json:"ttl"`         // 事务超时时间
}

// --------------------------------------------------------------------------------
type Tx struct {
	id string
	mu sync.Mutex
	w  sync.WaitGroup

	hub *txHub // 用于主事务端维护各分支事务

	ttlCancel context.CancelFunc
	ttlCtx    context.Context

	txInfo         *TxInfo  // 当前事务的信息
	rootTxInfo     *TxInfo  // 主事务的信息
	tType          txType   // 事务类型
	status         txStatus // 事务状态
	ctx            context.Context
	confirmHandler func()
	cancelHandler  func()
}

func Begin(ctx context.Context, confirm func(), cancel func()) (*Tx, context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var t = &Tx{}
	t.id = uuid.New().String()
	t.ctx = ctx
	t.status = txStatusPending

	t.confirmHandler = confirm
	t.cancelHandler = cancel

	var rootTxInfo *TxInfo
	if t.ctx != nil {
		rootTxInfo = txInfoWithContext(t.ctx)
	}

	var ttl time.Time
	if m.timeout > 0 {
		ttl = time.Now().Add(m.timeout)
	}

	// 构建当前事务的信息
	t.txInfo = &TxInfo{}
	t.txInfo.TxId = t.id
	t.txInfo.ServerName = m.serverName
	t.txInfo.ServerAddr = m.serverAddr
	t.txInfo.ServerUUID = m.serverUUID
	t.txInfo.TTL = ttl

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
		t.txInfo.TTL = rootTxInfo.TTL

		// 发消息告知主事务，有分支事务建立
		if err := t.register(); err != nil {
			return nil, nil, err
		}
	}

	// 添加事务到管理器中
	m.addTx(t)

	// 启动超时处理
	t.setupTTL()

	return t, t.Context(), nil
}

func (this *Tx) Id() string {
	return this.id
}

func (this *Tx) IdPath() string {
	if this.tType == txTypeBranch {
		return fmt.Sprintf("%s - %s", this.rootTxInfo.TxId, this.id)
	}
	return this.id
}

func (this *Tx) Context() context.Context {
	return this.ctx
}

// --------------------------------------------------------------------------------
func (this *Tx) setupTTL() {
	if this.ttlCancel != nil {
		this.ttlCancel()
		this.ttlCancel = nil
	}

	if this.txInfo.TTL.IsZero() {
		return
	}

	this.ttlCtx, this.ttlCancel = context.WithDeadline(context.Background(), this.txInfo.TTL)

	go this.runTTL()
}

func (this *Tx) runTTL() {
	for {
		select {
		case <-this.ttlCtx.Done():
			if this.ttlCtx.Err() == context.DeadlineExceeded {
				this.ttlHandler()
			}
			return
		}
	}
}

func (this *Tx) ttlHandler() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	this.ttlCancel = nil
	this.ttlCtx = nil
	this.status = txStatusPendingCancel

	if this.tType == txTypeRoot {
		for _, tx := range this.hub.getTxList() {

			go m.timeoutTx(tx.txInfo, this.txInfo)

			if tx.status == txStatusPending {
				this.w.Done()
			}
		}
	} else {
		go m.timeoutTx(this.rootTxInfo, this.txInfo)
	}

	// 事务自身进行 cancel 操作
	this.cancelTx()
}

// --------------------------------------------------------------------------------
// register 分支事务向主事务注册（分）
func (this *Tx) register() error {
	return m.registerTx(this.rootTxInfo, this.txInfo)
}

// registerTxHandler 分支事务向主事务发起注册消息之后，主事务添加分支事务信息（主）
func (this *Tx) registerTxHandler(tx *Tx) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if tx.status != txStatusPending {
		return
	}

	if tx == nil {
		return
	}

	if this.hub == nil {
		this.hub = newTxHub()
	}
	this.hub.addTx(tx)

	this.w.Add(1)
}

// --------------------------------------------------------------------------------
// Commit 提交事务
// 分支事务 - 发消息告知主事务，将该分支事务的状态调整为提交状态
// 主事务 - 等待所有分支事务的消息，并判断所有分支事务的状态，决定是 cancel 还是 confirm，向所有的分支事务派发对应的消息
func (this *Tx) Commit() (err error) {
	if this.status != txStatusPending {
		return
	}

	if this.tType == txTypeBranch {
		// 如果是分支事务，则向主事务发送消息
		if err = m.commitTx(this.rootTxInfo, this.txInfo); err == nil {
			this.status = txStatusPendingConfirm
		} else {
			this.status = txStatusPendingCancel
		}
	} else {
		// 等待所有的分支事务操作完成
		this.w.Wait()

		this.mu.Lock()
		defer this.mu.Unlock()

		if this.status != txStatusPending {
			return
		}

		var txList = this.hub.getTxList()

		// 检查分支事务的状态，如果有状态不为 commit 的，则进行 cancel 操作，否则进行 confirm 操作
		var shouldCancel = false
		for _, tx := range txList {
			if tx.status != txStatusPendingConfirm {
				shouldCancel = true
				break
			}
		}

		// TODO 验证分支事务是否活跃

		if shouldCancel {
			// 通知所有的分支事务，进行 cancel 操作
			for _, tx := range txList {
				m.cancelTx(tx.txInfo, this.txInfo)
			}
			this.status = txStatusPendingCancel
			this.cancelTx()
		} else {
			// 通知所有的分支事务，进行 confirm 操作
			for _, tx := range txList {
				m.confirmTx(tx.txInfo, this.txInfo)
			}

			this.status = txStatusPendingConfirm
			this.confirmTx()
		}
	}

	return err
}

// commitTxHandler 分支事务提交之后，主事务将其维护的分支事务的状态标记为等待提交（主）
func (this *Tx) commitTxHandler(txId string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	var tx = this.hub.getTx(txId)
	if tx != nil && tx.status == txStatusPending {
		tx.status = txStatusPendingConfirm
		this.w.Done()
	}
}

// --------------------------------------------------------------------------------
// cancelTxHandler 取消事务（分）
func (this *Tx) cancelTxHandler() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	// 分支事务收到主事务的 cancel 消息之后，将本事务的状态标记为 pending cancel
	this.status = txStatusPendingCancel

	this.cancelTx()
}

func (this *Tx) cancelTx() {
	if this.ttlCancel != nil {
		this.ttlCancel()
	}

	// 如果状态不为 pending cancel, 则不能进行 cancel 操作
	if this.status != txStatusPendingCancel {
		return
	}

	if this.cancelHandler != nil && this.status != txStatusCancel {
		this.cancelHandler()
	}

	this.status = txStatusCancel

	m.delTx(this.id)
}

// --------------------------------------------------------------------------------
// confirmTxHandler 确认事务（分）
func (this *Tx) confirmTxHandler() {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	// 分支事务收到主事务的 confirm 消息之后，将本事务的状态标记为 pending confirm
	this.status = txStatusPendingConfirm

	this.confirmTx()
}

func (this *Tx) confirmTx() {
	if this.ttlCancel != nil {
		this.ttlCancel()
	}

	// 如果状态不为 pending confirm, 则不能进行 confirm 操作
	if this.status != txStatusPendingConfirm {
		return
	}

	if this.confirmHandler != nil && this.status != txStatusConfirm {
		this.confirmHandler()
	}

	this.status = txStatusConfirm

	m.delTx(this.id)
}

// --------------------------------------------------------------------------------
// Rollback 回滚事务
// 分支事务 - 发消息告知主事务，将该分支事务的状态调整为回滚状态
// 主事务 - 等待所有分支事务的消息，接收到所有分支事务的消息之后，向所有的分支事务派发 cancel 消息
func (this *Tx) Rollback() (err error) {
	if this.tType == txTypeBranch {
		// 分支事务

		this.mu.Lock()
		defer this.mu.Unlock()

		if this.status != txStatusPending {
			return
		}

		err = m.rollbackTx(this.rootTxInfo, this.txInfo)
	} else {
		// 主事务

		// 等待所有的分支事务操作完成
		this.w.Wait()

		this.mu.Lock()
		defer this.mu.Unlock()

		if this.status != txStatusPending {
			return
		}

		var txList = this.hub.getTxList()

		// 通知所有的分支事务，进行 cancel 操作
		for _, tx := range txList {
			// 只向已提交的分支事务发送 cancel 消息
			if tx.status == txStatusPendingConfirm {
				m.cancelTx(tx.txInfo, this.txInfo)
			}
		}
	}

	// 接将当前事务标记为等待取消
	this.status = txStatusPendingCancel

	this.cancelTx()

	return err
}

// rollbackTxHandler 分支事务回滚之后，主事务将其维护的分支事务的状态标记为等待取消（主）
func (this *Tx) rollbackTxHandler(txId string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	var tx = this.hub.getTx(txId)
	if tx != nil && tx.status == txStatusPending {
		tx.status = txStatusPendingCancel
		this.w.Done()
	}
}

// --------------------------------------------------------------------------------
func (this *Tx) timeoutTxHandler(txId string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	// 已经取消和确认的事务不能再进行操作
	if this.status == txStatusCancel || this.status == txStatusConfirm {
		return
	}

	// 如果是分支事务收到主事务的超时消息，则进行 cancel 操作
	if this.tType == txTypeBranch {
		this.status = txStatusPendingCancel
		this.cancelTx()
		return
	}

	// 如果是主事务收到分支事务的超时消息，则改变分支事务的状态
	var tx = this.hub.getTx(txId)
	if tx != nil {
		var oldStatus = tx.status
		tx.status = txStatusPendingCancel

		if oldStatus == txStatusPending {
			this.w.Done()
		}
	}
}
