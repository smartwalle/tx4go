module github.com/smartwalle/tx4go/cmd

go 1.12

require (
	github.com/golang/protobuf v1.4.0
	github.com/micro/go-micro/v2 v2.9.1
	github.com/micro/go-plugins/registry/etcdv3/v2 v2.9.1
	github.com/micro/go-plugins/wrapper/trace/opentracing/v2 v2.9.1
	github.com/opentracing/opentracing-go v1.1.0
	github.com/smartwalle/jaeger4go v1.0.0 // indirect
	github.com/smartwalle/log4go v1.0.4 // indirect
	github.com/smartwalle/tx4go v0.0.0
	google.golang.org/grpc v1.26.0
)

replace github.com/smartwalle/tx4go => ../
