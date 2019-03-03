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
		if db.DB, err = sqlx.Connect("sqlite3", ":memory:"); err != nil {
			return merr.Wrap(err, db.ctx)
		}
		return db.init()
	})

	db.ctx = mrun.WithStopHook(db.ctx, func(context.Context) error {
		return db.DB.Close()
	})

	return mctx.WithChild(ctx, db.ctx), &db
}

func (db *db) init() error {
	mlog.Info("initializing tables", db.ctx)
	_, err := db.Exec(
		`CREATE TABLE peer_resources (
			addr TEXT,
			resource TEXT,
			state INTEGER,
			nonce INTEGER,
			PRIMARY KEY(addr, resource)
		);
	`)
	return merr.Wrap(err, db.ctx)
}

func (db *db) incomingMsg(msg Msg) error {
	_, err := db.Exec(
		`INSERT OR REPLACE INTO peer_resources
			SELECT newdata.* FROM
    			(SELECT
					? AS addr,
					? AS resource,
					? AS state,
					? AS nonce) AS newdata
    		LEFT JOIN peer_resources as olddata
				ON newdata.addr=olddata.addr
				AND newdata.resource=olddata.resource
    			WHERE newdata.nonce>olddata.nonce
				OR olddata.addr IS NULL;
	`, msg.Addr, msg.Resource, msg.MsgType, msg.Nonce)
	return merr.Wrap(err, db.ctx)
}
