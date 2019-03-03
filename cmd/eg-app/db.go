package main

import (
	"context"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
)

type db struct {
	ctx context.Context
	*sqlx.DB
}

func withDB(ctx context.Context) (context.Context, *db) {
	db := db{
		ctx: mctx.NewChild(ctx, "db"),
	}

	db.ctx = mrun.WithStartHook(db.ctx, func(context.Context) error {
		mlog.Info("creating sqlite db", db.ctx)
		var err error
		db.DB, err = sqlx.Connect("sqlite3", ":memory:")
		return merr.Wrap(err, db.ctx)
	})

	db.ctx = mrun.WithStopHook(db.ctx, func(context.Context) error {
		return db.DB.Close()
	})

	return mctx.WithChild(ctx, db.ctx), &db
}
