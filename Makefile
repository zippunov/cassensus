
init-cluster:
	./scripts/start_cluster.sh

db-reset:
	ccm node1 cqlsh -f db/cassandra/init.cql

test:
	go test -v