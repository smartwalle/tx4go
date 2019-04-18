package service

import (
	"fmt"
	"github.com/smartwalle/tx4go"
)

// --------------------------------------------------------------------------------
type StockService struct {
}

func (this *StockService) DecreaseStock(t *tx4go.Tx) {
	// 注册事务
	var tx = t.Begin(this.DecreaseConfirm, this.DecreaseCancel)

	// do something

	tx.Commit()
}

func (this *StockService) DecreaseConfirm() {
	fmt.Println("stock confirm")
}

func (this *StockService) DecreaseCancel() {
	fmt.Println("stock cancel")
}
