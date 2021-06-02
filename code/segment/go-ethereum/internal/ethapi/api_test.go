package ethapi

import (
	"context"
	"github.com/ethereum/go-ethereum/common"
	"reflect"
	"testing"
)

func TestPublicTransactionPoolAPI_SendTransaction(t *testing.T) {
	type fields struct {
		b         Backend
		nonceLock *AddrLocker
	}
	type args struct {
		ctx  context.Context
		args SendTxArgs
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    common.Hash
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &PublicTransactionPoolAPI{
				b:         tt.fields.b,
				nonceLock: tt.fields.nonceLock,
			}
			got, err := s.SendTransaction(tt.args.ctx, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("SendTransaction() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SendTransaction() got = %v, want %v", got, tt.want)
			}
		})
	}
}