package main

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mediocregopher/mediocre-go-lib/mctx"
	"github.com/mediocregopher/mediocre-go-lib/merr"
	"github.com/mediocregopher/mediocre-go-lib/mlog"
	"github.com/mediocregopher/mediocre-go-lib/mrun"
	"github.com/mediocregopher/mediocre-go-lib/mtime"
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
			lastTS REAL,
			PRIMARY KEY(addr, resource)
		);
	`)
	return merr.Wrap(err, db.ctx)
}

func (db *db) recordHave(msg msgEvent) error {
	_, err := db.Exec(
		`INSERT OR REPLACE INTO peer_resources
			SELECT newdata.* FROM
    			(SELECT
					? AS addr,
					? AS resource,
					? AS state,
					? AS nonce,
					? AS lastTS) AS newdata
    		LEFT JOIN peer_resources as olddata
				ON newdata.addr=olddata.addr
				AND newdata.resource=olddata.resource
    			WHERE newdata.nonce>olddata.nonce
				OR olddata.addr IS NULL;`,
		msg.Addr, msg.Resource, msg.MsgType, msg.Nonce,
		mtime.NewTS(msg.TS).Float64(),
	)
	return merr.Wrap(err, db.ctx)
}

// peers returns the addresses of all peers from which a message was received
// since the given time.
//
// TODO index on lastTS
func (db *db) peers(since time.Time) ([]string, error) {
	var addrs []string
	err := db.Select(&addrs,
		`SELECT DISTINCT addr FROM peer_resources
		WHERE lastTS >= ?
		AND state = 0;`,
		mtime.NewTS(since).Float64(),
	)
	return addrs, merr.Wrap(err, db.ctx)
}

func (db *db) peersWith(resource string, since time.Time) ([]string, error) {
	var addrs []string
	err := db.Select(&addrs,
		`SELECT DISTINCT addr FROM peer_resources
		WHERE resource = ?
		AND lastTS >= ?
		AND state = 0;`,
		resource, mtime.NewTS(since).Float64(),
	)
	return addrs, merr.Wrap(err, db.ctx)
}
