package main

import "github.com/smartwalle/tx4go/sample/service"

func main() {
	var order = &service.OrderService{}
	order.Create()
}
