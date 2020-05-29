package cassensus

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

func buildResult(q *gocqlx.Queryx) (bool, Lease, error) {
	m := map[string]interface{}{}
	applied, err := q.MapScanCAS(m)
	l := Lease{}
	_ = mapstructure.Decode(m, &l)
	return applied, l, err
}

type dao struct {
	table   string
	session *gocql.Session
}

func (d dao) Acquire(name, owner string) (bool, Lease, error) {
	stmt, names := qb.Insert(d.table).Columns("name", "owner").Unique().ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":  name,
		"owner": owner,
	})
	return buildResult(q)
}

func (d dao) AcquireExt(name, owner, payload string, ttl int) (bool, Lease, error) {
	dur := time.Second * time.Duration(ttl)
	stmt, names := qb.Insert(d.table).Columns("name", "owner", "payload").TTL(dur).Unique().ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":    name,
		"owner":   owner,
		"payload": payload,
	})
	return buildResult(q)
}

func (d dao) Renew(name, owner string) (bool, Lease, error) {
	stmt, names := qb.Update(d.table).Set("owner").Where(qb.Eq("name")).If(qb.Eq("owner")).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":  name,
		"owner": owner,
	})
	return buildResult(q)
}

func (d dao) RenewExt(name, owner, payload string, ttl int) (bool, Lease, error) {
	dur := time.Second * time.Duration(ttl)
	stmt, names := qb.Update(d.table).Set("owner", "payload").Where(qb.Eq("name")).TTL(dur).If(qb.Eq("owner")).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":    name,
		"owner":   owner,
		"payload": payload,
	})
	return buildResult(q)
}

func (d dao) Release(name, owner string) (bool, Lease, error) {
	stmt, names := qb.Delete(d.table).Where(qb.Eq("name")).If(qb.Eq("owner")).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":  name,
		"owner": owner,
	})
	return buildResult(q)
}

func (d dao) Read(name string) (Lease, error) {
	stmt, names := qb.Select(d.table).Columns("writetime(owner) as created", "name", "payload", "owner").Where(qb.Eq("name")).Limit(1).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name": name,
	})
	rslt := []Lease{}
	err := q.SerialConsistency(gocql.Serial).SelectRelease(&rslt)
	if len(rslt) == 0 {
		return Lease{}, nil
	}
	return rslt[0], err
}
