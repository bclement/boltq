package boltq

import (
	"bytes"
	"encoding/binary"
	"fmt"

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
TxDeleteIndex deletes the specified index
*/
func (ds DataStore) DeleteIndex(collection, index []byte) error {
	err := ds.Update(func(tx *bolt.Tx) error {
		return TxDeleteIndex(tx, collection, index)
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
TxDeleteIndex deletes the specified index in the given transaction
*/
func TxDeleteIndex(tx *bolt.Tx, collection, index []byte) error {
	indexKey := createIndexKey(collection, index)
	return tx.DeleteBucket(indexKey)
}

/*
Index records the composite key associated with the value for use in index queries
*/
func TxIndex(tx *bolt.Tx, collection, index, value []byte, compositeKey [][]byte) error {
	indexKey := createIndexKey(collection, index)
	b, err := tx.CreateBucketIfNotExists(indexKey)
	if err == nil {
		var serializedKey []byte
		var serializedSet []byte
		serializedKey = SerializeComposite(compositeKey)
		if err == nil {
			serializedSet, err = addToSetValue(b.Get(value), serializedKey)
			if err == nil {
				b.Put(value, serializedSet)
			}
		}
	}

	return err
}

/*
addToSetValue adds value to a serialized set
a serialized version of the updated set is returned
*/
func addToSetValue(serializedSet, value []byte) ([]byte, error) {
	var err error
	var rval []byte
	var set map[string]bool
	var keys []string
	if serializedSet == nil {
		set = make(map[string]bool)
	} else {
		set, keys, err = deserializeSet(serializedSet)
	}
	if err == nil {
		valStr := string(value)
		_, exists := set[valStr]
		if !exists {
			set[valStr] = true
			keys = append(keys, valStr)
		}
		rval = serializeSet(set, keys)
	}

	return rval, err
}

/*
serializeSet serializes an ordered set to bytes
*/
func serializeSet(set map[string]bool, keys []string) []byte {
	sizeBuff := make([]byte, 8)
	var buff bytes.Buffer
	for _, keyStr := range keys {
		key := []byte(keyStr)
		size := len(key)
		buff.Grow(size + 8)
		binary.PutVarint(sizeBuff, int64(size))
		buff.Write(sizeBuff)
		buff.Write(key)
	}
	return buff.Bytes()
}

/*
deserializeSet deserializes an ordered set from bytes
the set is returned as a map along with the key ordering
*/
func deserializeSet(serialized []byte) (map[string]bool, []string, error) {
	var err error
	rval := make(map[string]bool)
	var keys []string
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
			keyStr := string(key)
			_, exists := rval[keyStr]
			if !exists {
				keys = append(keys, keyStr)
				rval[keyStr] = true
			}
		}
	}
	return rval, keys, err
}

/*
serializeComposite serializes the composite key to be stored in an index
*/
func SerializeComposite(compositeKey [][]byte) []byte {
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
func DeserializeComposite(serialized []byte) ([][]byte, error) {
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
	return rval, err
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
	case NOT:
		results, err = getNot(state, results)
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

	c := state.b.Cursor()
	for _, queryKey := range term.keys {
		key, val := c.Seek(queryKey)

		if bytes.Equal(key, queryKey) {
			results, err = getVal(state, key, val, results)
		}
	}

	return results, err
}

/*
getNot gets results at the current level whose keys are not in the term
*/
func getNot(state qState, results [][]byte) ([][]byte, error) {
	var err error

	term := state.terms[state.level]

	c := state.b.Cursor()
	for key, val := c.First(); key != nil; key, val = c.Next() {
		if !in(key, term.keys) {
			results, err = getVal(state, key, val, results)
		}
	}

	return results, err
}

/*
in returns true if val is in arr
*/
func in(val []byte, arr [][]byte) bool {
	rval := false
	for _, entry := range arr {
		if bytes.Equal(val, entry) {
			rval = true
			break
		}
	}
	return rval
}

/*
TxIndexMatchAny queries for items using the given index and the values that were indexed
*/
func TxIndexMatchAny(tx *bolt.Tx, collection, index []byte, values ...[]byte) ([][]byte, error) {
	var err error
	var rval [][]byte

	indexKey := createIndexKey(collection, index)
	b := tx.Bucket(indexKey)

	if b != nil {
		for i := 0; err == nil && i < len(values); i += 1 {
			value := values[i]
			serializedSet := b.Get(value)
			if serializedSet != nil {
				var keys []string
				_, keys, err = deserializeSet(serializedSet)
				if err == nil {
					for _, keyStr := range keys {
						key := []byte(keyStr)
						compositeKey, err := DeserializeComposite(key)
						if err != nil {
							return rval, err
						}
						rval, err = collectResults(tx, collection, compositeKey, rval)
						if err != nil {
							return rval, err
						}
					}
				}
			}
		}
	}

	return rval, err
}

/*
TxIndexMatchAll queries for items using the given index and the values that were indexed
*/
func TxIndexMatchAll(tx *bolt.Tx, collection, index []byte, values ...[]byte) ([][]byte, error) {
	var err error
	var rval [][]byte

	indexKey := createIndexKey(collection, index)
	b := tx.Bucket(indexKey)

	if b != nil {
		var matched map[string]bool
		for i := 0; err == nil && i < len(values); i += 1 {
			value := values[i]
			serializedSet := b.Get(value)
			if serializedSet != nil {
				var set map[string]bool
				set, _, err = deserializeSet(serializedSet)
				if err == nil {
					if matched == nil {
						matched = set
					} else {
						for key, _ := range matched {
							inSet, _ := set[key]
							if !inSet {
								matched[key] = false
							}
						}
					}
				}
			}
		}
		for keyStr, inSet := range matched {
			if err == nil && inSet {
				key := []byte(keyStr)
				compositeKey, err := DeserializeComposite(key)
				if err == nil {
					rval, err = collectResults(tx, collection, compositeKey, rval)
				}
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

/* TODO support query deletes */

func TxDelete(tx *bolt.Tx, collection []byte, keys ...[]byte) (err error) {
	b := tx.Bucket(collection)
	if b != nil {
		err = recursiveDelete(b, keys)
	}
	return
}

func recursiveDelete(b *bolt.Bucket, keys [][]byte) (err error) {
	numKeys := len(keys)
	if b == nil || numKeys < 1 {
		return
	}
	c := b.Cursor()
	k, v := c.Seek(keys[0])
	if k != nil && bytes.Equal(k, keys[0]) {
		/* return key matched, we found something */
		if v == nil {
			/* nil value signals that key is nested bucket */
			if numKeys == 1 {
				/* no more keys to filter on, prune bucket tree */
				err = b.DeleteBucket(k)
			} else {
				nextBucket := b.Bucket(k)
				err = recursiveDelete(nextBucket, keys[1:])
			}
		} else {
			if numKeys == 1 {
				err = b.Delete(k)
			} else {
				err = fmt.Errorf("More keys specified than nested buckets available")
			}
		}
	}
	return
}
