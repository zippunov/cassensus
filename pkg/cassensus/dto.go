package cassensus

type Lease struct {
	Name    string `db:"name"`
	Owner   string `db:"owner"`
	Payload string `db:"payload"`
	Created int64  `db:"created"`
}
