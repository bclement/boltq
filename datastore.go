package boltq

import (
	"bytes"

	"github.com/boltdb/bolt"
)

/*
DataStore adds composite key support to BoltDB
*/
type DataStore struct {
	*bolt.DB
}

/*
Store places the value in the datastore. The collection key is used for the root bucket
of the storage while the composite key is used as the path to the final storage loaction
*/
func (ds DataStore) Store(collection []byte, compositeKey [][]byte, value []byte) error {

	err := ds.Update(func(tx *bolt.Tx) error {
		return TxStore(tx, collection, compositeKey, value)
	})

	return err
}

/*
TxStore places the value in the datastore that tx came from. The collection key is used
for the root bucket of the storage while the composite key is used as the path to the
final storage loaction
*/
func TxStore(tx *bolt.Tx, collection []byte, compositeKey [][]byte, value []byte) error {
	b, err := tx.CreateBucketIfNotExists(collection)
	lastKeyIndex := len(compositeKey) - 1
	for i := 0; err == nil && i < lastKeyIndex; i += 1 {
		b, err = b.CreateBucketIfNotExists(compositeKey[i])
	}
	if err == nil {
		err = b.Put(compositeKey[lastKeyIndex], value)
	}
	return err
}

/*
qState is composed of data that represents the state at a specific level of a query
*/
type qState struct {
	b     *bolt.Bucket
	terms []*Term
	level int
}

/*
next gets the next state level for the query
*/
func (s qState) next(b *bolt.Bucket) qState {
	return qState{b, s.terms, s.level + 1}
}

/*
TxQuery executes the query in the provided transaction
*/
func TxQuery(tx *bolt.Tx, query *Query) ([][]byte, error) {
	var rval [][]byte
	var err error

	b := tx.Bucket(query.Collection)
	if b != nil {
		state := qState{b, query.Terms, 0}
		rval, err = getByTerm(state, rval)
	}

	return rval, err
}

/*
getByTerm recursively collects query results accoring to the term of the current query level
*/
func getByTerm(state qState, results [][]byte) ([][]byte, error) {
	var err error
	var term *Term
	if state.level >= len(state.terms) {
		term = Any()
	} else {
		term = state.terms[state.level]
	}

	switch term.op {
	case ANY:
		results, err = getAny(state, results)
	case EQ:
		results, err = getEq(state, results)
	case RANGE:
		results, err = getRange(state, results)
	case IN:
		results, err = getIn(state, results)
	}

	return results, err
}

/*
getVal gets the results stored at the key/val pair which could be a bucket or item.
A nil val signifies that key points to nested bucket
*/
func getVal(state qState, key, val []byte, results [][]byte) ([][]byte, error) {
	var err error
	if val != nil {
		results = append(results, val)
	} else {
		nested := state.b.Bucket(key)
		results, err = getByTerm(state.next(nested), results)
	}
	return results, err
}

/*
getAny gets all results at the current level of the query
*/
func getAny(state qState, results [][]byte) ([][]byte, error) {
	var err error
	c := state.b.Cursor()
	for key, val := c.First(); key != nil; key, val = c.Next() {
		results, err = getVal(state, key, val, results)
	}

	return results, err
}

/*
getEq get results for a specific key at the current level
*/
func getEq(state qState, results [][]byte) ([][]byte, error) {
	var err error

	term := state.terms[state.level]
	queryKey := term.keys[0]
	c := state.b.Cursor()
	key, val := c.Seek(queryKey)

	if bytes.Equal(key, queryKey) {
		results, err = getVal(state, key, val, results)
	}

	return results, err
}

/*
getRange gets results for a range of keys at the current level
*/
func getRange(state qState, results [][]byte) ([][]byte, error) {
	var err error

	term := state.terms[state.level]
	lower := term.keys[0]
	upper := term.keys[1]
	c := state.b.Cursor()
	limit, _ := c.Seek(upper)
	key, val := c.Seek(lower)
	for ; key != nil && !bytes.Equal(key, limit); key, val = c.Next() {
		results, err = getVal(state, key, val, results)
	}

	return results, err
}

/*
getIn gets results for a set of keys at the curent level
*/
func getIn(state qState, results [][]byte) ([][]byte, error) {
	var err error

	term := state.terms[state.level]

	for _, queryKey := range term.keys {
		c := state.b.Cursor()
		key, val := c.Seek(queryKey)

		if bytes.Equal(key, queryKey) {
			results, err = getVal(state, key, val, results)
		}
	}

	return results, err
}
