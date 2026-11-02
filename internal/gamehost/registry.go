package gamehost

import (
	"fmt"
	"strconv"
	"sync"
)

// Registry finds a running scene from outside its actor, as a timer handler
// must. A scene adds itself in its Factory and removes itself in OnRetire.
type Registry[T comparable] struct {
	mu sync.RWMutex
	m  map[string]T
}

// Put records a scene.
func (r *Registry[T]) Put(key string, v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]T{}
	}
	r.m[key] = v
}

// Get finds a scene.
func (r *Registry[T]) Get(key string) (T, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.m[key]
	return v, ok
}

// Remove forgets a scene, unless a newer one has taken its key.
func (r *Registry[T]) Remove(key string, v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m[key] == v {
		delete(r.m, key)
	}
}

// AdmitOwner is the Gate of a per-player scene, whose key is the owner's uid.
// The client names the key of each binding, so this is what keeps one player out
// of another's scene.
func AdmitOwner(key string, uid int64) error {
	if key != strconv.FormatInt(uid, 10) {
		return fmt.Errorf("uid %d opened %s", uid, key)
	}
	return nil
}
