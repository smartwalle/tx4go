package tx4go

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/client"
	"github.com/smartwalle/tx4go/pb"
	"sync"
	"time"
)

const (
	kDefaultRetryCount = 3
	kDefaultRetryDelay = 256 * time.Millisecond
	kDefaultTimeout    = 15 * time.Second
)

var (
	ErrTxNotFound           = errors.New("tx4go: not found")
	ErrNotAllowed           = errors.New("tx4go: not allowed")
	ErrUninitializedManager = errors.New("tx4go: uninitialized tx manager")
)

var m *Manager

type Manager struct {
	isInit     bool
	hub        *txHub
	serverUUID string
	serverName string
	serverAddr string
	service    micro.Service
	codec      Codec

	timeout    time.Duration
	retryDelay time.Duration
	retryCount int
}

func (this *Manager) addTx(tx *Tx) {
	this.hub.addTx(tx)
}

func (this *Manager) delTx(id string) {
	this.hub.delTx(id)
}

func (this *Manager) getTx(id string) *Tx {
	return this.hub.getTx(id)
}

func (this *Manager) run() {
	pb.RegisterTxHandler(this.service.Server(), this)
}

// --------------------------------------------------------------------------------
// registerTx 分支事务向主事务发起注册事务的请求
func (this *Manager) registerTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId
	req.FromServerUUID = fromTx.ServerUUID
	req.FromServerName = fromTx.ServerName
	req.FromServerAddr = fromTx.ServerAddr

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Register(ctx, req, client.WithAddress(toTx.ServerAddr))
	return err
}

// Register 主事务处理分支事务发起的注册事务的请求
func (this *Manager) Register(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx == nil {
		return ErrTxNotFound
	}

	var bTx = &Tx{}
	bTx.id = req.FromId
	bTx.tType = txTypeBranch
	bTx.status = txStatusPending
	bTx.txInfo = &TxInfo{}
	bTx.txInfo.TxId = req.FromId
	bTx.txInfo.ServerUUID = req.FromServerUUID
	bTx.txInfo.ServerName = req.FromServerName
	bTx.txInfo.ServerAddr = req.FromServerAddr

	if ok := tx.registerTxHandler(bTx); ok == false {
		return ErrNotAllowed
	}

	return nil
}

// --------------------------------------------------------------------------------
// commitTx 分支事务向主事务发起提交事务的请求
func (this *Manager) commitTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Commit(ctx, req, client.WithAddress(toTx.ServerAddr))
	return err
}

// Commit 主事务处理分支事务发起的提交事务的请求
func (this *Manager) Commit(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx == nil {
		return ErrTxNotFound
	}

	tx.commitTxHandler(req.FromId)

	return nil
}

// --------------------------------------------------------------------------------
// rollbackTx 分支事务向主事务发起回滚事务的请求
func (this *Manager) rollbackTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Rollback(ctx, req, client.WithAddress(toTx.ServerAddr))
	return err
}

// Rollback 主事务处理分支事务发起的回滚事务的请求
func (this *Manager) Rollback(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx == nil {
		return ErrTxNotFound
	}

	tx.rollbackTxHandler(req.FromId)

	return nil
}

// --------------------------------------------------------------------------------
// cancelTx 主事务向分支事务发起取消事务的请求
func (this *Manager) cancelTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Cancel(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

// Cancel 分支事务处理主事务发起的取消事务的请求
func (this *Manager) Cancel(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == req.FromId {
		tx.cancelTxHandler()
	}
	return nil
}

// --------------------------------------------------------------------------------
// confirmTx 主事务向分支事务发起确认事务的请求
func (this *Manager) confirmTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Confirm(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

// Confirm 分支事务处理主事务发起的确认事务的请求
func (this *Manager) Confirm(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == req.FromId {
		tx.confirmTxHandler()
	}
	return nil
}

// --------------------------------------------------------------------------------
func (this *Manager) timeoutTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Timeout(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

func (this *Manager) Timeout(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx == nil {
		return ErrTxNotFound
	}

	tx.timeoutTxHandler(req.FromId)

	return nil
}

// --------------------------------------------------------------------------------
var initOnce sync.Once

func Init(s micro.Service, opts ...Option) {
	initOnce.Do(func() {
		m = &Manager{}
		m.hub = newTxHub()
		m.service = s
		m.serverUUID = uuid.New().String()

		if m.service != nil {
			if m.service.Server() != nil {
				m.serverName = m.service.Server().Options().Name
				m.serverAddr = m.service.Server().Options().Address
			}
		}

		m.timeout = kDefaultTimeout
		m.retryCount = kDefaultRetryCount
		m.retryDelay = kDefaultRetryDelay

		for _, opt := range opts {
			opt.Apply(m)
		}

		if m.codec == nil {
			m.codec = &DefaultCodec{}
		}

		m.run()

		m.isInit = true

		logger.Printf("初始化事务管理器 %s 成功 \n", m.serverUUID)
	})
}
