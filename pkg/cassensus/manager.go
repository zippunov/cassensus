package cassensus

import (
	"log"

	"github.com/gocql/gocql"
)

func NewCassensus() *CassensusManager {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "cassensus"
	cluster.Consistency = gocql.LocalQuorum
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	return &CassensusManager{
		dao: dao{
			table:   "leases",
			session: session,
		},
	}
}

type CassensusManager struct {
	dao dao
}

func (d *CassensusManager) Acquire(name, owner string) (bool, Lease, error) {
	return d.dao.Acquire(name, owner)
}

func (d *CassensusManager) Renew(name, owner string) (bool, Lease, error) {
	return d.dao.Renew(name, owner)
}

func (d *CassensusManager) Release(name, owner string) (bool, Lease, error) {
	return d.dao.Release(name, owner)
}

func (d *CassensusManager) Read(name string) (Lease, error) {
	return d.dao.Read(name)
}
