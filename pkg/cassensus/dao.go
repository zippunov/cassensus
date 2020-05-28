package cassensus

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx"
	"github.com/scylladb/gocqlx/qb"
)

func getString(m map[string]interface{}, key string) string {
	if val, ok := m[key]; !ok {
		return ""
	} else if str, ok := val.(string); ok {
		return str
	}
	return ""
}

func getIn64(m map[string]interface{}, key string) int64 {
	if val, ok := m[key]; !ok {
		return 0
	} else if i, ok := val.(int64); ok {
		return i
	}
	return 0
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
	m := map[string]interface{}{}
	applied, err := q.MapScanCAS(m)
	l := Lease{
		Name:    getString(m, "name"),
		Owner:   getString(m, "owner"),
		Value:   getString(m, "value"),
		Created: getIn64(m, "created"),
	}
	return applied, l, err
}

func (d dao) Renew(name, owner string) (bool, Lease, error) {
	stmt, names := qb.Update(d.table).Set("owner").Where(qb.Eq("name")).If(qb.Eq("owner")).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":  name,
		"owner": owner,
	})
	m := map[string]interface{}{}
	applied, err := q.MapScanCAS(m)
	l := Lease{
		Name:    getString(m, "name"),
		Owner:   getString(m, "owner"),
		Value:   getString(m, "value"),
		Created: getIn64(m, "created"),
	}
	return applied, l, err
}

func (d dao) Release(name, owner string) (bool, Lease, error) {
	stmt, names := qb.Delete(d.table).Where(qb.Eq("name")).If(qb.Eq("owner")).ToCql()
	q := gocqlx.Query(d.session.Query(stmt), names).BindMap(qb.M{
		"name":  name,
		"owner": owner,
	})
	m := map[string]interface{}{}
	ok, err := q.MapScanCAS(m)
	l := Lease{
		Name:    getString(m, "name"),
		Owner:   getString(m, "owner"),
		Value:   getString(m, "value"),
		Created: getIn64(m, "created"),
	}
	return ok, l, err
}

func (d dao) Read(name string) (Lease, error) {
	stmt, names := qb.Select(d.table).Columns("writetime(owner) as created", "name", "value", "owner").Where(qb.Eq("name")).Limit(1).ToCql()
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
