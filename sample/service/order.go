package service

import (
	"github.com/smartwalle/tx4go"
)

// --------------------------------------------------------------------------------
type OrderService struct {
	stockServ *StockService
	pointServ *PointService
}

// Create 创建订单
func (this *OrderService) Create() error {
	// 注册事务
	var tx = tx4go.Begin(this.Confirm, this.Cancel)

	//do something
	//if false {
	//	tx.Rollback()
	//}

	this.stockServ.DecreaseStock(tx)
	this.pointServ.DecreasePoint(tx)

	tx.Commit()

	return nil
}

// Confirm 确认创建订单
func (this *OrderService) Confirm() {
}

// Cancel 取消创建订单
func (this *OrderService) Cancel() {
}
