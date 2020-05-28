package cassensus

type Lease struct {
	Name    string `db:"name"`
	Owner   string `db:"owner"`
	Value   string `db:"value"`
	Created int64  `db:"created"`
}
