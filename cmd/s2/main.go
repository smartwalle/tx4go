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
	"github.com/smartwalle/tx4go/cmd/s1/s1pb"
	"github.com/smartwalle/tx4go/cmd/s2/s2pb"
	"time"
)

func main() {
	var cfg, err = jaeger4go.Load("./cfg.yaml")
	if err != nil {
		log4go.Println(err)
		return
	}

	closer, err := cfg.InitGlobalTracer("s2")
	if err != nil {
		log4go.Println(err)
		return
	}
	defer closer.Close()

	var s = micro.NewService(
		micro.Server(grpc_server.NewServer(server.Address("192.168.1.99:8912"))),
		micro.Client(grpc_client.NewClient(client.PoolSize(10))),
		micro.RegisterTTL(time.Second*10),
		micro.RegisterInterval(time.Second*5),
		micro.Registry(etcdv3.NewRegistry()),
		micro.Name("tx-s2"),
		micro.WrapHandler(wo.NewHandlerWrapper(opentracing.GlobalTracer())),
		micro.WrapClient(wo.NewClientWrapper(opentracing.GlobalTracer())),
		micro.WrapHandler(tx4go.NewHandlerWrapper()),
		micro.WrapCall(tx4go.NewCallWrapper()),
	)

	tx4go.SetLogger(nil)
	var m = tx4go.NewManager(s)

	s2pb.RegisterS2Handler(s.Server(), &S2{s: s, m: m})

	s.Run()
}

type S2 struct {
	s micro.Service
	m tx4go.Manager
}

func (this *S2) Call(ctx context.Context, req *s2pb.Req, rsp *s2pb.Rsp) error {
	fmt.Println("s2 收到请求")

	span, ctx := opentracing.StartSpanFromContext(ctx, "s2-call")
	span.LogKV("s2-call-key", "s2-call-value")
	span.Finish()

	tx, ctx, err := this.m.Begin(ctx, func() {
		log4go.Println("confirm")
	}, func() {
		log4go.Errorln("cancel")
	})

	if err != nil {
		log4go.Errorln("tx error", err)
		return err
	}

	var ts = s1pb.NewS1Service("tx-s1", this.s.Client())
	ts.Call(ctx, &s1pb.Req{})

	tx.Commit()

	return nil
}
