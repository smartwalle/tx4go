package tx4go

import (
	"context"
	"encoding/json"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server"
)

const (
	kTxInfo = "tx-info"
)

func NewHandlerWrapper() server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) error {
			md, ok := metadata.FromContext(ctx)
			if ok {
				infoStr, ok := md[kTxInfo]
				if ok && infoStr != "" {
					var info *TxInfo
					if err := json.Unmarshal([]byte(infoStr), &info); err == nil {
						ctx = NewContext(ctx, info)
					}
				}
			}

			return h(ctx, req, rsp)
		}
	}
}

func NewCallWrapper() client.CallWrapper {
	return func(cf client.CallFunc) client.CallFunc {
		return func(ctx context.Context, node *registry.Node, req client.Request, rsp interface{}, opts client.CallOptions) error {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				md = metadata.Metadata{}
				ctx = metadata.NewContext(ctx, md)
			}

			if _, ok = md[kTxInfo]; !ok {
				var info, _ = FromContext(ctx)
				if info != nil {
					if infoBytes, err := json.Marshal(info); err == nil {
						md[kTxInfo] = string(infoBytes)
					}
				}
			}

			return cf(ctx, node, req, rsp, opts)
		}
	}
}
