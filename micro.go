package tx4go

import (
	"context"
	"encoding/json"
	"github.com/micro/go-micro/v2/client"
	"github.com/micro/go-micro/v2/metadata"
	"github.com/micro/go-micro/v2/registry"
	"github.com/micro/go-micro/v2/server"
)

const (
	kTxInfo = "Tx-Info"
)

func NewHandlerWrapper() server.HandlerWrapper {
	return func(h server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, rsp interface{}) error {
			infoStr, ok := metadata.Get(ctx, kTxInfo)
			if ok && infoStr != "" {
				var info *TxInfo
				if err := json.Unmarshal([]byte(infoStr), &info); err == nil {
					ctx = NewContext(ctx, info)
				}
			}
			return h(ctx, req, rsp)
		}
	}
}

func NewCallWrapper() client.CallWrapper {
	return func(cf client.CallFunc) client.CallFunc {
		return func(ctx context.Context, node *registry.Node, req client.Request, rsp interface{}, opts client.CallOptions) error {
			_, ok := metadata.Get(ctx, kTxInfo)

			if !ok {
				var info, _ = FromContext(ctx)
				if info != nil {
					if infoBytes, err := json.Marshal(info); err == nil {
						ctx = metadata.Set(ctx, kTxInfo, string(infoBytes))
					}
				}
			}

			return cf(ctx, node, req, rsp, opts)
		}
	}
}
