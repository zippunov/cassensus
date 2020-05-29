package cassensus

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/xid"
	"github.com/zippunov/cassensus/pkg/cassensus"
)

var key = "key_" + xid.New().String()

var ownerID = "owner_" + xid.New().String()
var guestID = "owner_" + xid.New().String()
var cass = cassensus.NewCassensus()

func TestAcquire(t *testing.T) {
	ok, _, err := cass.Acquire(key, ownerID)
	if !ok {
		t.Errorf("Got NOT ok result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
}

func TestRenewByOwner(t *testing.T) {
	data1, err := cass.Read(key)
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	ok, _, err := cass.Renew(key, ownerID)
	if !ok {
		t.Errorf("Got NOT ok result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	data3, err := cass.Read(key)
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if data1.Created >= data3.Created {
		t.Errorf("Lease time is not updated")
	}
}

func TestReadByOwner(t *testing.T) {
	data, err := cass.Read(key)
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if data.Name != key {
		t.Errorf("Key mismatch")
	}
	if data.Owner != ownerID {
		t.Errorf("Owner mismatch")
	}
	if data.Created == 0 {
		t.Errorf("Empty created field")
	}
}

func TestReleaseByOwner(t *testing.T) {
	ok, _, err := cass.Release(key, ownerID)
	if !ok {
		t.Errorf("Got NOT ok result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
}

func TestAcquireWhileLocked(t *testing.T) {
	ok, _, err := cass.Acquire(key, ownerID)
	if !ok {
		t.Errorf("Got NOT ok result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}

	ok, lease, err := cass.Acquire(key, guestID)
	if ok {
		t.Errorf("Got false OK result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if lease.Owner != ownerID {
		t.Errorf("Got wrong  lease owner %s", lease.Owner)
	}
}

func TestRenewWhileLocked(t *testing.T) {
	ok, lease, err := cass.Renew(key, guestID)
	if ok {
		t.Errorf("Got false OK result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if lease.Owner != ownerID {
		t.Errorf("Got wrong  lease owner %s", lease.Owner)
	}
}

func TestReleaseWhileLocked(t *testing.T) {
	ok, lease, err := cass.Release(key, guestID)
	if ok {
		t.Errorf("Got false OK result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if lease.Owner != ownerID {
		t.Errorf("Got wrong  lease owner %s", lease.Owner)
	}
}

func TestAcquireExtended(t *testing.T) {
	var k = "key_" + xid.New().String()
	ok, _, err := cass.AcquireExt(k, ownerID, "Payload #1", 1)
	if !ok {
		t.Errorf("Got NOT ok result")
	}
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	data, err := cass.Read(k)
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if data.Name != k {
		t.Errorf("Key mismatch")
	}
	if data.Owner != ownerID {
		t.Errorf("Owner mismatch")
	}
	if data.Created == 0 {
		t.Errorf("Empty created field")
	}
	if data.Payload != "Payload #1" {
		t.Errorf("Wrong payload")
	}
	time.Sleep(time.Second * 1)
	data, err = cass.Read(k)
	if err != nil {
		t.Errorf("Got and error %v", err)
	}
	if data.Name != "" || data.Owner != "" || data.Payload != "" || data.Created != 0 {
		t.Errorf("Custom expire did not work")
	}
}

func TestAsyncLocking(t *testing.T) {
	key := "key_" + xid.New().String()
	wg := sync.WaitGroup{}
	wg.Add(100)
	count := 0
	for i := 0; i < 100; i++ {
		go func() {
			owner := "owner_" + xid.New().String()
			ok, _, err := cass.Acquire(key, owner)
			if err != nil {
				t.Errorf("Got and error %v", err)
			}
			if ok {
				count++
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if count > 1 {
		t.Errorf("Got wrong successfull locks count %d", count)
	}
}
