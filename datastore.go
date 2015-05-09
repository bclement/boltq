package boltq

import (
	"bytes"
	"encoding/binary"

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
Index records the composite key associated with the value for use in index queries
*/
func (ds DataStore) Index(collection, index, value []byte, compositeKey [][]byte) error {
	err := ds.Update(func(tx *bolt.Tx) error {
		return TxIndex(tx, collection, index, value, compositeKey)
	})

	return err
}

/*
createIndexKey creates a key used to access the bucket that stores index values
*/
func createIndexKey(collection, index []byte) []byte {
	indexKey := make([]byte, 0, len(collection)+len(index))
	indexKey = append(indexKey, collection...)
	return append(indexKey, index...)
}

/*
Index records the composite key associated with the value for use in index queries
*/
func TxIndex(tx *bolt.Tx, collection, index, value []byte, compositeKey [][]byte) error {
	indexKey := createIndexKey(collection, index)
	b, err := tx.CreateBucketIfNotExists(indexKey)
	if err == nil {
		b.Put(value, serializeComposite(compositeKey))
	}

	return err
}

/*
serializeComposite serializes the composite key to be stored in an index
*/
func serializeComposite(compositeKey [][]byte) []byte {
	sizeBuff := make([]byte, 8)
	var buff bytes.Buffer
	for _, key := range compositeKey {
		size := len(key)
		buff.Grow(size + 8)
		binary.PutVarint(sizeBuff, int64(size))
		buff.Write(sizeBuff)
		buff.Write(key)
	}
	return buff.Bytes()
}

/*
deserializeComposite deserializes a composite key that was stored in an index
*/
func deserializeComposite(serialized []byte) [][]byte {
	var err error
	var rval [][]byte
	sizeBuff := make([]byte, 8)
	buff := bytes.NewBuffer(serialized)
	count := 0
	for count < len(serialized) {
		var length int
		length, err = buff.Read(sizeBuff)
		if err == nil {
			count += length
			size, _ := binary.Varint(sizeBuff)
			key := make([]byte, size)
			length, err = buff.Read(key)
			count += length
			rval = append(rval, key)
		}
	}
	return rval
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

/*
TxIndexQuery queries for items using the given index and the values that were indexed
*/
func TxIndexQuery(tx *bolt.Tx, collection, index []byte, values ...[]byte) ([][]byte, error) {
	var err error
	var rval [][]byte

	indexKey := createIndexKey(collection, index)
	b := tx.Bucket(indexKey)

	if b != nil {
		for i := 0; err == nil && i < len(values); i += 1 {
			value := values[i]
			storedKey := b.Get(value)
			if storedKey != nil {
				compositeKey := deserializeComposite(storedKey)
				rval, err = collectResults(tx, collection, compositeKey, rval)
			}
		}
	}

	return rval, err
}

/*
collectResults appends values found at composite key to index query results
*/
func collectResults(tx *bolt.Tx, collection []byte, compositeKey, results [][]byte) ([][]byte, error) {
	var err error

	q := NewQuery(collection, EqAll(compositeKey)...)
	res, err := TxQuery(tx, q)
	for j := 0; err == nil && j < len(res); j += 1 {
		results = append(results, res...)
	}

	return results, err
}