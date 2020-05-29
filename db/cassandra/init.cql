DROP KEYSPACE IF EXISTS cassensus;

CREATE KEYSPACE IF NOT EXISTS cassensus WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};

use cassensus;

CREATE TABLE leases (
      name text PRIMARY KEY,
      owner text,
      payload text
 ) with default_time_to_live = 30;