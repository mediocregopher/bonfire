package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

type db struct {
	*sqlx.DB
}

func withDB(ctx context.Context) (context.Context, *db) {
	var db db

	ctx = mrun.WithStartHook(ctx, func(context.Context) error {
		mlog.Info("creating sqlite db", ctx)
		var err error
		db.DB, err = sqlx.Connect("sqlite3", ":memory:")
		return merr.Wrap(err, ctx)
	})

	ctx = mrun.WithStopHook(ctx, func(context.Context) error {
		return db.DB.Close()
	})

	return ctx, &db
}
