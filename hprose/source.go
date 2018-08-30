package hprose

import (
	"context"
	"github.com/hprose/hprose-golang/rpc"
	"github.com/vlorc/hprose-gateway-core/invoker"
	"github.com/vlorc/hprose-gateway-types"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type hproseSource struct {
	service *types.Service
	once    sync.Once
	invoker invoker.Invoker
	mx      sync.Mutex
	val     atomic.Value
}

func (h *hproseSource) SetService(service *types.Service) {
	if nil == service {
		return
	}
	ok := nil != h.service && h.service.Url != service.Url
	h.service = service
	if ok {
		h.__close()
	}
}

func (h *hproseSource) Service() *types.Service {
	return h.service
}

func (p *hproseSource) Reset() types.Source {
	p.Close()
	*p = zero
	return p
}

func (h *hproseSource) Close() error {
	h.__close()
	return nil
}

func (h *hproseSource) __close() {
	cli := h.__client()
	if nil == cli {
		return
	}
	h.val.Store(nil)
	time.AfterFunc(time.Minute, func() {
		cli.Close()
	})
}

func (h *hproseSource) __client() rpc.Client {
	cli, ok := h.val.Load().(rpc.Client)
	if !ok {
		return nil
	}
	return cli
}

func (h *hproseSource) client() rpc.Client {
	cli := h.__client()
	if nil != cli {
		return cli
	}
	h.mx.Lock()
	defer h.mx.Unlock()
	if cli = h.__client(); nil != cli {
		return cli
	}
	if cli = rpc.NewClient(h.service.Url); nil != cli {
		h.val.Store(cli)
		return cli
	}
	return nil
}

func (h *hproseSource) Endpoint() types.Endpoint {
	return h
}

func (h *hproseSource) Invoke(ctx context.Context, method string, params []reflect.Value) ([]reflect.Value, error) {
	if cli := h.client(); nil != cli {
		return cli.Invoke(method, params, settings)
	}
	return nil, io.EOF
}
