// Package shard maps a uid to its database. Logical shards are fixed forever;
// growing means moving logical shards between physical databases, so no row
// ever needs rehashing.
package shard

import (
	"database/sql"
	"fmt"
)

// LogicalShards is permanent. Changing it would re-home every player.
const LogicalShards = 1024

// Logical returns the logical shard for a uid.
func Logical(uid int64) int {
	return int(uint64(uid) % LogicalShards)
}

// Router holds the logical-to-physical mapping.
type Router struct {
	dbs []*sql.DB // Indexed by logical shard.
}

// NewEven spreads the logical shards evenly over dbs.
func NewEven(dbs []*sql.DB) (*Router, error) {
	if len(dbs) == 0 {
		return nil, fmt.Errorf("shard: no databases")
	}
	r := &Router{dbs: make([]*sql.DB, LogicalShards)}
	for i := range r.dbs {
		r.dbs[i] = dbs[i*len(dbs)/LogicalShards]
	}
	return r, nil
}

// For returns the database holding uid.
func (r *Router) For(uid int64) *sql.DB { return r.dbs[Logical(uid)] }
