package main

import (
	"context"
	"fmt"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-plugins/registry/etcdv3"
	wo "github.com/micro/go-plugins/wrapper/trace/opentracing"
	"github.com/smartwalle/jaeger4go"
	"github.com/smartwalle/log4go"
	pks_client "github.com/smartwalle/pks/plugins/client/pks_grpc"
	pks_server "github.com/smartwalle/pks/plugins/server/pks_grpc"
	"github.com/smartwalle/tx4go"
	"github.com/smartwalle/tx4go/sample/s2/s2pb"
	"time"
)

func main() {
	var cfg, err = jaeger4go.Load("./cfg.yaml")
	if err != nil {
		log4go.Println(err)
		return
	}

	closer, err := cfg.InitGlobalTracer("s3")
	if err != nil {
		log4go.Println(err)
		return
	}
	defer closer.Close()

	var s = micro.NewService(
		micro.Server(pks_server.NewServer(server.Address("192.168.1.99:8913"))),
		micro.Client(pks_client.NewClient(client.PoolSize(10))),
		micro.RegisterTTL(time.Second*10),
		micro.RegisterInterval(time.Second*5),
		micro.Registry(etcdv3.NewRegistry()),
		micro.Name("tx-s3"),
		micro.WrapHandler(wo.NewHandlerWrapper()),
		micro.WrapClient(wo.NewClientWrapper()),
	)

	tx4go.SetLogger(nil)
	tx4go.Init(s)

	time.AfterFunc(time.Second*2, func() {
		fmt.Println("向 s2 发起请求")

		tx, ctx, err := tx4go.Begin(context.Background(), func() {
			log4go.Println("confirm")
		}, func() {
			log4go.Errorln("cancel")
		})

		if err != nil {
			log4go.Errorln("tx error", err)
			return
		}

		var ts = s2pb.NewS2Service("tx-s2", s.Client())
		ts.Call(ctx, &s2pb.Req{})

		tx.Commit()
	})

	s.Run()
}
