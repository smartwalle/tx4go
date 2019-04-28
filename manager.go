package tx4go

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/smartwalle/pks"
	"sync"
	"time"
)

const (
	kDefaultRetryCount = 3
	kDefaultRetryDelay = 256 * time.Millisecond
	kDefaultTimeout    = 5 * time.Second
)

var (
	kErrTxNotFound = errors.New("tx: not found")
	kErrNotAllowed = errors.New("tx: not allowed")
)

var m *Manager

type Manager struct {
	mu         sync.Mutex
	txList     map[string]*Tx
	serverUUID string
	serverName string
	serverAddr string
	service    *pks.Service

	timeout    time.Duration
	retryDelay time.Duration
	retryCount int
}

func (this *Manager) addTx(tx *Tx) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if tx != nil {
		this.txList[tx.id] = tx
	}
}

func (this *Manager) delTx(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.txList, id)
}

func (this *Manager) getTx(id string) *Tx {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.txList[id]
}

func (this *Manager) run() {
	this.service.Handle(getRegisterTxPath(this.serverUUID), this.registerTxHandler)
	this.service.Handle(getCommitTxPath(this.serverUUID), this.commitTxHandler)
	this.service.Handle(getRollbackTxPath(this.serverUUID), this.rollbackTxHandler)
	this.service.Handle(getCancelTxPath(this.serverUUID), this.cancelTxHandler)
	this.service.Handle(getConfirmTxPath(this.serverUUID), this.confirmTxHandler)
}

// --------------------------------------------------------------------------------
// registerTx 分支事务向主事务发起注册事务的请求
func (this *Manager) registerTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxReqParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	param.FromServerUUID = fromTx.ServerUUID
	param.FromServerName = fromTx.ServerName
	param.FromServerAddr = fromTx.ServerAddr

	_, err = this.request(context.Background(), toTx.ServerAddr, getRegisterTxPath(toTx.ServerUUID), param)
	if err != nil {
		return err
	}

	return nil
}

// registerTxHandler 主事务处理分支事务发起的注册事务的请求
func (this *Manager) registerTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxReqParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param == nil {
		return kErrTxNotFound
	}

	var tx = this.getTx(param.ToId)
	if tx == nil {
		return kErrTxNotFound
	}

	var bTx = &Tx{}
	bTx.id = param.FromId
	bTx.tType = txTypeBranch
	bTx.status = txStatusPending
	bTx.txInfo = &TxInfo{}
	bTx.txInfo.TxId = param.FromId
	bTx.txInfo.ServerUUID = param.FromServerUUID
	bTx.txInfo.ServerName = param.FromServerName
	bTx.txInfo.ServerAddr = param.FromServerAddr

	tx.registerTx(bTx)

	return nil
}

// --------------------------------------------------------------------------------
// commitTx 分支事务向主事务发起提交事务的请求
func (this *Manager) commitTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxReqParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	_, err = this.request(context.Background(), toTx.ServerAddr, getCommitTxPath(toTx.ServerUUID), param)
	if err != nil {
		return err
	}

	return nil
}

// commitTxHandler 主事务处理分支事务发起的提交事务的请求
func (this *Manager) commitTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxReqParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param == nil {
		return kErrTxNotFound
	}

	var tx = this.getTx(param.ToId)
	if tx == nil {
		return kErrTxNotFound
	}

	tx.commitTx(param.FromId)

	return nil
}

// --------------------------------------------------------------------------------
// rollbackTx 分支事务向主事务发起回滚事务的请求
func (this *Manager) rollbackTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxReqParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	_, err = this.request(context.Background(), toTx.ServerAddr, getRollbackTxPath(toTx.ServerUUID), param)
	if err != nil {
		return err
	}

	return nil
}

// rollbackTxHandler 主事务处理分支事务发起的回滚事务的请求
func (this *Manager) rollbackTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxReqParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param == nil {
		return kErrTxNotFound
	}

	var tx = this.getTx(param.ToId)
	if tx == nil {
		return kErrTxNotFound
	}

	tx.rollbackTx(param.FromId)

	return nil
}

// --------------------------------------------------------------------------------
// cancelTx 主事务向分支事务发起取消事务的请求
func (this *Manager) cancelTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxReqParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId

	_, err = this.request(context.Background(), toTx.ServerAddr, getCancelTxPath(toTx.ServerUUID), param)
	if err != nil {
		return err
	}
	return nil
}

// cancelTxHandler 分支事务处理主事务发起的取消事务的请求
func (this *Manager) cancelTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxReqParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	var tx = this.getTx(param.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == param.FromId {
		tx.cancelTx()
	}

	return nil
}

// --------------------------------------------------------------------------------
// confirmTx 主事务向分支事务发起确认事务的请求
func (this *Manager) confirmTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxReqParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId

	_, err = this.request(context.Background(), toTx.ServerAddr, getConfirmTxPath(toTx.ServerUUID), param)
	if err != nil {
		return err
	}

	return nil
}

// confirmTxHandler 分支事务处理主事务发起的确认事务的请求
func (this *Manager) confirmTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxReqParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	var tx = this.getTx(param.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == param.FromId {
		return tx.confirmTx()
	}

	return nil
}

// --------------------------------------------------------------------------------
func (this *Manager) request(ctx context.Context, address, path string, param interface{}) (rsp *pks.Response, err error) {
	paramBytes, err := json.Marshal(param)
	if err != nil {
		return nil, err
	}

	for i := 0; i <= this.retryCount; i++ {
		if i != 0 {
			time.Sleep(this.retryDelay)
		}
		if rsp, err = this.service.RequestAddress(ctx, address, path, nil, paramBytes); err == nil {
			return rsp, nil
		}
	}

	return rsp, err
}

// --------------------------------------------------------------------------------
var initOnce sync.Once

func Init(s *pks.Service, opts ...Option) {
	initOnce.Do(func() {
		m = &Manager{}
		m.txList = make(map[string]*Tx)
		m.service = s
		m.serverUUID = uuid.New().String()
		m.serverName = s.ServerName()
		m.serverAddr = s.ServerAddress()

		m.timeout = kDefaultTimeout
		m.retryCount = kDefaultRetryCount
		m.retryDelay = kDefaultRetryDelay

		for _, opt := range opts {
			opt.Apply(m)
		}

		m.run()
	})
}

// --------------------------------------------------------------------------------
func getRegisterTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%service-register", serverUUID)
}

func getCommitTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%service-commit", serverUUID)
}

func getRollbackTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%service-rollback", serverUUID)
}

func getCancelTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%service-cancel", serverUUID)
}

func getConfirmTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%service-confirm", serverUUID)
}

// --------------------------------------------------------------------------------
type TxReqParam struct {
	ToId           string `json:"to_id"`                      // 目标事务id
	FromId         string `json:"from_id"`                    // 请求来源事务的 id
	FromServerUUID string `json:"from_server_uuid,omitempty"` // 请求来源事务的 server uuid
	FromServerName string `json:"from_server_name,omitempty"` // 请求来源事务的 server name
	FromServerAddr string `json:"from_server_addr,omitempty"` // 请求来源事务的 server addr
}
