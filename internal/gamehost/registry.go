package gamehost

import (
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

// IsOwner reports whether key is this player's uid. The client names the key of
// each binding, so a per-player scene must check it before showing anything.
func (p *Player) IsOwner(key string) bool { return key == strconv.FormatInt(p.UID, 10) }
