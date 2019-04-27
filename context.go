package tx4go

import (
	"context"
	"encoding/json"
	"github.com/micro/go-micro/metadata"
)

const (
	kTxInfo = "tx-info"
)

func txInfoWithContext(ctx context.Context) *TxInfo {

	md, ok := metadata.FromContext(ctx)
	if ok == false {
		return nil
	}

	infoStr, ok := md[kTxInfo]
	if ok == false {
		return nil
	}

	var info *TxInfo
	if err := json.Unmarshal([]byte(infoStr), &info); err != nil {
		return nil
	}
	return info
}

func txInfoToContext(ctx context.Context, info *TxInfo) context.Context {
	if info == nil {
		return ctx
	}
	infoBytes, err := json.Marshal(info)
	if err != nil {
		return ctx
	}
	md, _ := metadata.FromContext(ctx)
	if md == nil {
		md = metadata.Metadata{}
	}
	md[kTxInfo] = string(infoBytes)
	return metadata.NewContext(ctx, md)
}
