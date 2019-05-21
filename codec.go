package tx4go

import (
	"context"
	"encoding/json"
	mm "github.com/micro/go-micro/metadata"
	gm "google.golang.org/grpc/metadata"
)

type Codec interface {
	Encode(ctx context.Context, info *TxInfo) context.Context

	Decode(ctx context.Context) (*TxInfo, error)
}

const (
	kTxInfo = "tx-info"
)

// --------------------------------------------------------------------------------
type DefaultCodec struct {
}

func (this *DefaultCodec) Encode(ctx context.Context, info *TxInfo) context.Context {
	if info == nil {
		return ctx
	}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return ctx
	}

	// 写入 micro context
	md, ok := mm.FromContext(ctx)
	if ok == false {
		md = mm.Metadata{}
	}
	md[kTxInfo] = string(infoBytes)
	ctx = mm.NewContext(ctx, md)

	// 写入 grpc context
	gmd, ok := gm.FromIncomingContext(ctx)
	if ok == false {
		gmd = gm.New(nil)
	}
	outMD, _ := gm.FromOutgoingContext(ctx)
	for key, values := range outMD {
		gmd.Set(key, values...)
	}
	gmd.Set(kTxInfo, string(infoBytes))

	return gm.NewOutgoingContext(ctx, gmd)
}

func (this *DefaultCodec) Decode(ctx context.Context) (*TxInfo, error) {
	var infoStr string

	md, ok := mm.FromContext(ctx)
	if ok {
		infoStr = md[kTxInfo]
		if infoStr == "" {
			ok = false
		}
	}

	if ok == false {
		// 从 grpc context 读取
		gmd, ok := gm.FromIncomingContext(ctx)
		if ok == false {
			return nil, nil
		}

		infoStrList := gmd[kTxInfo]
		if len(infoStrList) == 0 {
			return nil, nil
		}
		infoStr = infoStrList[0]
	}

	if infoStr == "" {
		return nil, nil
	}

	var info *TxInfo
	if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
		return nil, err
	}
	return info, nil
}
