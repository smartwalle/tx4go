package tx4go

import "context"

type txInfo struct{}

func FromContext(ctx context.Context) (*TxInfo, bool) {
	md := ctx.Value(txInfo{})
	if md == nil {
		return nil, false
	}
	return md.(*TxInfo), true
}

func NewContext(ctx context.Context, info *TxInfo) context.Context {
	return context.WithValue(ctx, txInfo{}, info)
}
