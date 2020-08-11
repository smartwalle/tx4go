package tx4go

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/micro/go-micro/v2"
	"github.com/micro/go-micro/v2/client"
	"github.com/opentracing/opentracing-go"
	"github.com/smartwalle/tx4go/pb"
	"time"
)

const (
	kDefaultRetryCount = 3
	kDefaultRetryDelay = 256 * time.Millisecond
	kDefaultTimeout    = 15 * time.Second
)

var (
	ErrTxNotFound = errors.New("tx4go: not found")
	ErrNotAllowed = errors.New("tx4go: not allowed")
)

type Manager interface {
	Begin(ctx context.Context, confirm func(), cancel func()) (*Tx, context.Context, error)
}

func NewManager(s micro.Service, opts ...Option) Manager {
	var m = &txManager{}
	m.hub = newTxHub()
	m.service = s

	if m.service != nil {
		if m.service.Server() != nil {
			m.serverUUID = m.service.Server().Options().Id
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

	m.run()

	logger.Printf("初始化事务管理器 %s 成功 \n", m.serverUUID)

	return m
}

type txManager struct {
	hub        *txHub
	serverUUID string
	serverName string
	serverAddr string
	service    micro.Service

	timeout    time.Duration
	retryDelay time.Duration
	retryCount int
}

func (this *txManager) addTx(tx *Tx) {
	this.hub.addTx(tx)
}

func (this *txManager) delTx(id string) {
	this.hub.delTx(id)
}

func (this *txManager) getTx(id string) *Tx {
	return this.hub.getTx(id)
}

func (this *txManager) run() {
	pb.RegisterTxHandler(this.service.Server(), this)
}

// registerTx 分支事务向主事务发起注册事务的请求
func (this *txManager) registerTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
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
func (this *txManager) Register(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
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

// commitTx 分支事务向主事务发起提交事务的请求
func (this *txManager) commitTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
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
func (this *txManager) Commit(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
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

// rollbackTx 分支事务向主事务发起回滚事务的请求
func (this *txManager) rollbackTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
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
func (this *txManager) Rollback(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
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

// cancelTx 主事务向分支事务发起取消事务的请求
func (this *txManager) cancelTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Cancel(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

// Cancel 分支事务处理主事务发起的取消事务的请求
func (this *txManager) Cancel(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == req.FromId {
		tx.cancelTxHandler()
	}
	return nil
}

// confirmTx 主事务向分支事务发起确认事务的请求
func (this *txManager) confirmTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Confirm(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

// Confirm 分支事务处理主事务发起的确认事务的请求
func (this *txManager) Confirm(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
	if req == nil {
		return ErrTxNotFound
	}

	var tx = this.getTx(req.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == req.FromId {
		tx.confirmTxHandler()
	}
	return nil
}

func (this *txManager) timeoutTx(ctx context.Context, toTx, fromTx *TxInfo) (err error) {
	var req = &pb.TxReq{}
	req.ToId = toTx.TxId
	req.FromId = fromTx.TxId

	var ts = pb.NewTxService(toTx.ServerName, this.service.Client())
	_, err = ts.Timeout(ctx, req, client.WithAddress(toTx.ServerAddr))
	return nil
}

func (this *txManager) Timeout(ctx context.Context, req *pb.TxReq, rsp *pb.TxRsp) error {
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

func (this *txManager) Begin(ctx context.Context, confirm func(), cancel func()) (*Tx, context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var t = &Tx{}
	t.id = uuid.New().String()
	t.status = txStatusPending
	t.hub = newTxHub()
	t.m = this

	t.confirmHandler = confirm
	t.cancelHandler = cancel

	var ttl time.Time
	if this.timeout > 0 {
		ttl = time.Now().Add(this.timeout)
	}

	// 构建当前事务的信息
	t.txInfo = &TxInfo{}
	t.txInfo.TxId = t.id
	t.txInfo.ServerName = this.serverName
	t.txInfo.ServerAddr = this.serverAddr
	t.txInfo.ServerUUID = this.serverUUID
	t.txInfo.TTL = ttl

	var rootTxInfo, _ = FromContext(ctx)

	span, ctx := opentracing.StartSpanFromContext(ctx, fmt.Sprintf("%s.Tx.Begin", this.serverName))
	span.Finish()

	t.ctx = ctx

	if rootTxInfo == nil {
		// 如果 rootTxInfo 为空，则表示当前事务为主事务
		t.tType = txTypeRoot

		// 将当前事务的信息放置到 ctx 中
		t.ctx = NewContext(ctx, t.txInfo)
	} else {
		// 如果 rootTxInfo 不为空，则表示当前事务为分支事务
		t.tType = txTypeBranch

		// 构建当前事务的主事务信息
		t.rootTxInfo = rootTxInfo
		t.txInfo.TTL = rootTxInfo.TTL

		// 发消息告知主事务，有分支事务建立
		if err := t.register(ctx); err != nil {
			return nil, nil, err
		}
	}

	// 添加事务到管理器中
	this.addTx(t)

	logger.Printf("事务 %s 创建成功 \n", t.idPath())

	// 启动超时处理
	t.setupTTL()

	return t, t.ctx, nil
}
