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
		micro.Server(pks_server.NewServer(server.Address("192.168.1.99:8931"))),
		micro.Client(pks_client.NewClient(client.PoolSize(10))),
		micro.RegisterTTL(time.Second*10),
		micro.RegisterInterval(time.Second*5),
		micro.Registry(etcdv3.NewRegistry()),
		micro.Name("tx-s3"),
	)

	tx4go.Init(s)

	time.AfterFunc(time.Second*2, func() {
		for i := 0; i < 1; i++ {
			tx, ctx, err := tx4go.Begin(context.Background(), func() {
				log4go.Println("confirm")
			}, func() {
				log4go.Errorln("cancel")
			})

			if err != nil {
				log4go.Errorln("tx error", err)
				return
			}

			log4go.Println("begin")

			s.Request(ctx, "tx-s2", "h2", nil, nil)

			if i%2 == 0 {
				tx.Commit()
			} else {
				tx.Rollback()
			}
			log4go.Println("done")

		}

	})

	s.Run()
}
