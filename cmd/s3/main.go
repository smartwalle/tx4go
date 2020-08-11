package main

import (
	"context"
	"fmt"
	"github.com/micro/go-micro/v2"
	"github.com/micro/go-micro/v2/client"
	grpc_client "github.com/micro/go-micro/v2/client/grpc"
	"github.com/micro/go-micro/v2/server"
	grpc_server "github.com/micro/go-micro/v2/server/grpc"
	"github.com/micro/go-plugins/registry/etcdv3/v2"
	wo "github.com/micro/go-plugins/wrapper/trace/opentracing/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/smartwalle/jaeger4go"
	"github.com/smartwalle/log4go"
	"github.com/smartwalle/tx4go"
	"github.com/smartwalle/tx4go/cmd/s2/s2pb"
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
		micro.Server(grpc_server.NewServer(server.Address("192.168.1.99:8913"))),
		micro.Client(grpc_client.NewClient(client.PoolSize(10))),
		micro.RegisterTTL(time.Second*10),
		micro.RegisterInterval(time.Second*5),
		micro.Registry(etcdv3.NewRegistry()),
		micro.Name("tx-s3"),
		micro.WrapHandler(wo.NewHandlerWrapper(opentracing.GlobalTracer())),
		micro.WrapClient(wo.NewClientWrapper(opentracing.GlobalTracer())),
		micro.WrapHandler(tx4go.NewHandlerWrapper()),
		micro.WrapCall(tx4go.NewCallWrapper()),
	)

	tx4go.SetLogger(nil)
	var m = tx4go.NewManager(s)

	time.AfterFunc(time.Second*2, func() {
		for i := 0; i < 1000; i++ {
			fmt.Println("向 s2 发起请求")

			span, ctx := opentracing.StartSpanFromContext(context.Background(), "s3-call")
			span.LogKV("s3-call-key", "s3-call-value")
			span.Finish()

			tx, ctx, err := m.Begin(ctx, func() {
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
		}
	})

	s.Run()
}
