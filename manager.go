package tx4go

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/smartwalle/pks"
	"sync"
)

var m *manager

type manager struct {
	mu         sync.Mutex
	txList     map[string]*Tx
	serverUUID string
	serverName string
	serverAddr string
	s          *pks.Service
}

func (this *manager) addTx(tx *Tx) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if tx != nil {
		this.txList[tx.id] = tx
	}
}

func (this *manager) delTx(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()

	delete(this.txList, id)
}

func (this *manager) getTx(id string) *Tx {
	this.mu.Lock()
	defer this.mu.Unlock()

	return this.txList[id]
}

func (this *manager) run() {
	this.s.Handle(getRegisterTxPath(this.serverUUID), this.registerTxHandler)
	this.s.Handle(getCommitTxPath(this.serverUUID), this.commitTxHandler)
	this.s.Handle(getRollbackTxPath(this.serverUUID), this.rollbackTxHandler)
	this.s.Handle(getCancelTxPath(this.serverUUID), this.cancelTxHandler)
	this.s.Handle(getConfirmTxPath(this.serverUUID), this.confirmTxHandler)
}

// --------------------------------------------------------------------------------
// registerTx 分支事务向主事务发起注册事务的请求
func (this *manager) registerTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	param.FromServerUUID = fromTx.ServerUUID
	param.FromServerName = fromTx.ServerName
	param.FromServerAddr = fromTx.ServerAddr

	paramBytes, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = this.s.RequestAddress(context.Background(), toTx.ServerAddr, getRegisterTxPath(toTx.ServerUUID), nil, paramBytes)
	if err != nil {
		return err
	}

	return nil
}

// registerTxHandler 主事务处理分支事务发起的注册事务的请求
func (this *manager) registerTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param != nil {
		var tx = this.getTx(param.ToId)
		if tx == nil {
			return errors.New("tx not found")
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
	}

	return nil
}

// --------------------------------------------------------------------------------
// commitTx 分支事务向主事务发起提交事务的请求
func (this *manager) commitTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	paramBytes, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = this.s.RequestAddress(context.Background(), toTx.ServerAddr, getCommitTxPath(toTx.ServerUUID), nil, paramBytes)
	if err != nil {
		return err
	}

	return nil
}

// commitTxHandler 主事务处理分支事务发起的提交事务的请求
func (this *manager) commitTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param != nil {
		var tx = this.getTx(param.ToId)
		if tx == nil {
			return errors.New("tx not exists")
		}

		tx.commitTx(param.FromId)
	}
	return nil
}

// --------------------------------------------------------------------------------
// rollbackTx 分支事务向主事务发起回滚事务的请求
func (this *manager) rollbackTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId
	//param.FromServerUUID = fromTx.ServerUUID
	//param.FromServerName = fromTx.ServerName
	//param.FromServerAddr = fromTx.ServerAddr

	paramBytes, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = this.s.RequestAddress(context.Background(), toTx.ServerAddr, getRollbackTxPath(toTx.ServerUUID), nil, paramBytes)
	if err != nil {
		return err
	}

	return nil
}

// rollbackTxHandler 主事务处理分支事务发起的回滚事务的请求
func (this *manager) rollbackTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	if param != nil {
		var tx = this.getTx(param.ToId)
		if tx == nil {
			return errors.New("tx not exists")
		}

		tx.rollbackTx(param.FromId)
	}
	return nil
}

// --------------------------------------------------------------------------------
// cancelTx 主事务向分支事务发起取消事务的请求
func (this *manager) cancelTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId

	paramBytes, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = this.s.RequestAddress(context.Background(), toTx.ServerAddr, getCancelTxPath(toTx.ServerUUID), nil, paramBytes)
	if err != nil {
		return err
	}
	return nil
}

// cancelTxHandler 分支事务处理主事务发起的取消事务的请求
func (this *manager) cancelTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxParam
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
func (this *manager) confirmTx(toTx, fromTx *TxInfo) (err error) {
	var param = &TxParam{}
	param.ToId = toTx.TxId
	param.FromId = fromTx.TxId

	paramBytes, err := json.Marshal(param)
	if err != nil {
		return err
	}

	_, err = this.s.RequestAddress(context.Background(), toTx.ServerAddr, getConfirmTxPath(toTx.ServerUUID), nil, paramBytes)
	if err != nil {
		return err
	}

	return nil
}

// confirmTxHandler 分支事务处理主事务发起的确认事务的请求
func (this *manager) confirmTxHandler(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
	var param *TxParam
	if err := json.Unmarshal(req.Body, &param); err != nil {
		return err
	}

	var tx = this.getTx(param.ToId)
	if tx != nil && tx.rootTxInfo != nil && tx.rootTxInfo.TxId == param.FromId {
		tx.confirmTx()
	}

	return nil
}

// --------------------------------------------------------------------------------
var initOnce sync.Once

func Init(s *pks.Service) {
	initOnce.Do(func() {
		m = &manager{}
		m.txList = make(map[string]*Tx)
		m.s = s
		m.serverUUID = uuid.New().String()
		m.serverName = s.ServerName()
		m.serverAddr = s.ServerAddress()
		m.run()
	})
}

func getRegisterTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%s-register", serverUUID)
}

func getCommitTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%s-commit", serverUUID)
}

func getRollbackTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%s-rollback", serverUUID)
}

func getCancelTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%s-cancel", serverUUID)
}

func getConfirmTxPath(serverUUID string) string {
	return fmt.Sprintf("tx-%s-confirm", serverUUID)
}

// --------------------------------------------------------------------------------
type TxParam struct {
	ToId           string `json:"to_id"`                      // 目标事务id
	FromId         string `json:"from_id"`                    // 请求来源事务 id
	FromServerUUID string `json:"from_server_uuid,omitempty"` // 请求来源事务的 server uuid
	FromServerName string `json:"from_server_name,omitempty"` // 请求来源事务的 server name
	FromServerAddr string `json:"from_server_addr,omitempty"` // 请求来源事务的 server addr
}
