package boltq

import (
	"bytes"
	"os"
	"path"
	"testing"
	"time"

	"github.com/boltdb/bolt"
)

const (
	DATA_DIR = "test_data"
)

func clean(dbfile string) {
	path := path.Join(DATA_DIR, dbfile)
	os.Remove(path)
}

func create(t *testing.T, dbfile string) DataStore {
	os.MkdirAll(DATA_DIR, 0777)
	db, err := bolt.Open(path.Join(DATA_DIR, dbfile), 0600, nil)
	if err != nil {
		t.Errorf("Unable to open test db file %v. %v", dbfile, err)
	}
	return DataStore{db}
}

func value(t *testing.T, offset time.Duration) []byte {
	now := time.Now()
	now = now.Add(offset)
	rval, err := now.MarshalBinary()
	if err != nil {
		t.Errorf("Unable to get bytes for time %v. %v.", now, err)
	}
	return rval
}

func composite(keys ...string) [][]byte {
	var rval [][]byte
	for _, key := range keys {
		rval = append(rval, []byte(key))
	}
	return rval
}

func TestSimple(t *testing.T) {
	testFile := "simple.db"
	ds := create(t, testFile)
	collection := []byte("simpleCol")
	key := []byte("simpleKey")
	val := value(t, 0)
	ds.Store(collection, [][]byte{key}, val)
	err := ds.View(func(tx *bolt.Tx) error {
		q := NewQuery(collection, Eq(key))
		res, err := TxQuery(tx, q)
		if err == nil {
			if len(res) != 1 {
				t.Errorf("Expected %v results, got %v", 1, len(res))
			}
			if !bytes.Equal(res[0], val) {
				t.Errorf("Expected %v back, got %v", val, res[0])
			}
		}

		return err
	})
	if err != nil {
		t.Errorf("Unable to view data: %v", err)
	}

	clean(testFile)
}

func assertQuery(t *testing.T, tx *bolt.Tx, q *Query, exp [][]byte, msg string) error {
	res, err := TxQuery(tx, q)
	if err == nil {
		assertValues(t, res, exp, msg)
	}
	return err
}

func assertValues(t *testing.T, res, exp [][]byte, msg string) {
	if len(res) != len(exp) {
		t.Errorf("%v: Expected %v results, got %v", msg, len(exp), len(res))
	} else {
		for i := range exp {
			if !bytes.Equal(exp[i], res[i]) {
				t.Errorf("%v: Expected %v back, got %v", msg, exp[i], res[i])
			}
		}
	}
}

func TestComplex(t *testing.T) {
	testFile := "complex.db"
	ds := create(t, testFile)
	collection := []byte("compCol")
	k0 := composite("0", "1", "0")
	v0 := value(t, 0)
	ds.Store(collection, k0, v0)
	k1 := composite("0", "2", "1")
	v1 := value(t, time.Minute)
	ds.Store(collection, k1, v1)
	k2 := composite("0", "3", "2")
	v2 := value(t, 2*time.Minute)
	ds.Store(collection, k2, v2)
	k3 := composite("0", "4", "3")
	v3 := value(t, 3*time.Minute)
	ds.Store(collection, k3, v3)
	k4 := composite("1", "A", "4")
	v4 := value(t, 4*time.Minute)
	ds.Store(collection, k4, v4)
	k5 := composite("1", "B", "5")
	v5 := value(t, 5*time.Minute)
	ds.Store(collection, k5, v5)
	k6 := composite("1", "C", "6")
	v6 := value(t, 6*time.Minute)
	ds.Store(collection, k6, v6)
	k7 := composite("1", "D", "7")
	v7 := value(t, 7*time.Minute)
	ds.Store(collection, k7, v7)

	q0 := NewQuery(collection, Any())
	exp0 := [][]byte{v0, v1, v2, v3, v4, v5, v6, v7}
	q1 := NewQuery(collection, EqAll(k0)...)
	exp1 := [][]byte{v0}
	q2 := NewQuery(collection, Eq([]byte("0")), Range([]byte("1"), []byte("3")))
	exp2 := [][]byte{v0, v1}
	q3 := NewQuery(collection, Any(), In([]byte("3"), []byte("D")))
	exp3 := [][]byte{v2, v7}
	q4 := NewQuery(collection, Not([]byte("1")), Range([]byte("1"), []byte("3")))

	err := ds.View(func(tx *bolt.Tx) error {
		assertQuery(t, tx, q0, exp0, "q0")
		assertQuery(t, tx, q1, exp1, "q1")
		assertQuery(t, tx, q2, exp2, "q2")
		assertQuery(t, tx, q3, exp3, "q3")
		assertQuery(t, tx, q4, exp2, "q4")
		return nil
	})
	if err != nil {
		t.Errorf("Unable to view data: %v", err)
	}

	clean(testFile)
}

func TestIndex(t *testing.T) {
	testFile := "index.db"
	ds := create(t, testFile)
	collection := []byte("col")
	index := []byte("idx")
	k0 := composite("0", "1", "0")
	v0 := value(t, 0)
	ds.Store(collection, k0, v0)
	k1 := composite("0", "2", "1")
	v1 := value(t, time.Minute)
	ds.Store(collection, k1, v1)
	k2 := composite("0", "3", "2")
	v2 := value(t, 2*time.Minute)
	ds.Store(collection, k2, v2)
	k3 := composite("0", "4", "3")
	v3 := value(t, 3*time.Minute)
	ds.Store(collection, k3, v3)

	err := ds.Index(collection, index, v0, k0)
	if err != nil {
		t.Errorf("Error indexing: %v", err)
	}
	err = ds.Index(collection, index, v0, k1)
	if err == nil {
		err = ds.View(func(tx *bolt.Tx) error {
			res, err := TxIndexQuery(tx, collection, index, v0, v1)
			if err == nil {
				assertValues(t, res, [][]byte{v0, v1}, "Bad results from index query")
			}
			return err
		})
	}
	if err != nil {
		t.Errorf("Error during test: %v", err)
	}

	clean(testFile)
}
