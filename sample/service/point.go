package service

import (
	"fmt"
	"github.com/smartwalle/tx4go"
)

// --------------------------------------------------------------------------------
type PointService struct {
}

func (this *PointService) DecreasePoint(t *tx4go.Tx) {
	// 注册事务
	var tx = t.Begin(this.DecreaseConfirm, this.DecreaseCancel)

	// do something

	tx.Rollback()
}

func (this *PointService) DecreaseConfirm() {
	fmt.Println("point confirm")
}

func (this *PointService) DecreaseCancel() {
	fmt.Println("point cancel")
}
