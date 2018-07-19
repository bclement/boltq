package boltq

type queryOp int

const (
	EQ = iota
	RANGE
	IN
	ANY
	NOT
)

/*
Term holds parameters of a query term
*/
type Term struct {
	op   queryOp
	keys [][]byte
}

/*
Eq creates a new equals query term
*/
func Eq(key []byte) *Term {
	return &Term{EQ, [][]byte{key}}
}

/*
Range creates a new range query term
*/
func Range(lower, upper []byte) *Term {
	return &Term{RANGE, [][]byte{lower, upper}}
}

/*
In creates a new In query term
*/
func In(keys ...[]byte) *Term {
	return &Term{IN, keys}
}

/*
Any creates a wildcard query term
*/
func Any() *Term {
	return &Term{ANY, nil}
}

/*
Not creates a NOT IN query term
*/
func Not(keys ...[]byte) *Term {
	return &Term{NOT, keys}
}

/*
EqAll creates Eq terms for all provided keys
*/
func EqAll(keys [][]byte) []*Term {
	var rval []*Term
	for _, key := range keys {
		rval = append(rval, Eq(key))
	}
	return rval
}

/*
Query holds all information specific to a db query
*/
type Query struct {
	Collection []byte
	Terms      []*Term
}

/*
NewQuery creates a new query from the provided parameters
*/
func NewQuery(collection []byte, terms ...*Term) *Query {
	return &Query{collection, terms}
}
