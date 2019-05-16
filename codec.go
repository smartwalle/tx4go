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
type MicroCodec struct {
}

func (this *MicroCodec) Encode(ctx context.Context, info *TxInfo) context.Context {
	if info == nil {
		return ctx
	}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return ctx
	}
	md, ok := mm.FromContext(ctx)
	if ok == false {
		md = mm.Metadata{}
	}
	md[kTxInfo] = string(infoBytes)
	return mm.NewContext(ctx, md)
}

func (this *MicroCodec) Decode(ctx context.Context) (*TxInfo, error) {
	md, ok := mm.FromContext(ctx)
	if ok == false {
		return nil, nil
	}

	infoStr, ok := md[kTxInfo]
	if ok == false {
		return nil, nil
	}

	var info *TxInfo
	if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
		return nil, err
	}
	return info, nil
}

// --------------------------------------------------------------------------------
type GRPCCodec struct {
}

func (this *GRPCCodec) Encode(ctx context.Context, info *TxInfo) context.Context {
	if info == nil {
		return ctx
	}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return ctx
	}
	md, ok := gm.FromIncomingContext(ctx)
	if ok == false {
		md = gm.New(nil)
	}

	outMD, _ := gm.FromOutgoingContext(ctx)
	for key, values := range outMD {
		md.Set(key, values...)
	}

	md.Set(kTxInfo, string(infoBytes))

	return gm.NewOutgoingContext(ctx, md)
}

func (this *GRPCCodec) Decode(ctx context.Context) (*TxInfo, error) {
	md, ok := gm.FromIncomingContext(ctx)
	if ok == false {
		return nil, nil
	}

	infoStrs, ok := md[kTxInfo]
	if ok == false {
		return nil, nil
	}

	if len(infoStrs) == 0 {
		return nil, nil
	}

	var info *TxInfo
	if err := json.Unmarshal([]byte(infoStrs[0]), &info); err != nil {
		return nil, err
	}

	return info, nil
}
