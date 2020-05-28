# Cassensus

Consensus on Cassandra POC

As described in the [Consensus on Cassandra](https://www.datastax.com/blog/2014/12/consensus-cassandra) article by Jake Luciani from Datastax. There is copy of the article in the doc directory.

Use CCM for local Cassandra cluster

To start local CCM cluster with 3 nodes
```bash
make init-cluster
... (will ask for password)
```

To init/reset DB
```
make db-reset
```

To start tests:

```
make test
```