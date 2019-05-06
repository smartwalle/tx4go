package main

import (
	"context"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-plugins/registry/etcdv3"
	"github.com/smartwalle/log4go"
	"github.com/smartwalle/pks"
	pks_client "github.com/smartwalle/pks/plugins/client/pks_grpc"
	pks_server "github.com/smartwalle/pks/plugins/server/pks_grpc"
	"github.com/smartwalle/tx4go"
	"time"
)

func main() {
	var s = pks.New(
		micro.Server(pks_server.NewServer(server.Address("192.168.1.99:8921"))),
		micro.Client(pks_client.NewClient(client.PoolSize(10))),
		micro.RegisterTTL(time.Second*60),
		micro.RegisterInterval(time.Second*40),
		micro.Registry(etcdv3.NewRegistry()),
		micro.Name("tx-s2"),
	)

	tx4go.Init(s)

	s.Handle("h2", func(ctx context.Context, req *pks.Request, rsp *pks.Response) error {
		log4go.Infof("-----收到来自 %s 的请求-----\n", req.FromService())

		tx, ctx, err := tx4go.Begin(ctx, func() {
			log4go.Println("confirm")
		}, func() {
			log4go.Errorln("cancel")
		})

		if err != nil {
			log4go.Infoln("tx error", tx, err)
			return nil
		}

		s.Request(ctx, "tx-s1", "h1", nil, nil)
		//tx.Commit()
		return nil
	})

	s.Run()
}
