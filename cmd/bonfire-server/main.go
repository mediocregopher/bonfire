package main

import (
	"context"

	"github.com/mediocregopher/bonfire"
	"github.com/mediocregopher/mediocre-go-lib/m"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mnet"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

func main() {
	ctx := m.ServiceContext()

	ctx, listener := mnet.WithListener(ctx,
		mnet.ListenerProtocol("udp"),
		mnet.ListenerAddr(":7890"),
	)

	srv := bonfire.NewServer()
	srvCtx, cancel := context.WithCancel(ctx)
	ctx = mrun.WithStartHook(ctx, func(context.Context) error {
		go func() {
			if err := srv.Serve(srvCtx, listener.PacketConn); err != context.Canceled {
				mlog.Fatal("error when serving", srvCtx, merr.Context(err))
			}
		}()
		return nil
	})

	ctx = mrun.WithStopHook(ctx, func(context.Context) error {
		cancel()
		return nil
	})

	m.StartWaitStop(ctx)
}
