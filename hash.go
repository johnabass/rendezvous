package rendezvous

import (
	"hash"
	"hash/fnv"
)

// Hasher is the strategy for hashing an object and a member together
type Hasher func(key, member []byte) uint64

// NewHasher32 uses a 32-bit hash constructor, such as fnv.New32a, as the basis
// for the returned Hasher implementation
func NewHasher32(hf func() hash.Hash32) Hasher {
	return func(key, member []byte) uint64 {
		h := hf()
		h.Write(key)
		h.Write(member)
		return uint64(h.Sum32())
	}
}

// NewHasher64 uses a 64-bit hash constructor, such as fnv.New64a, as the basis
// for the returned Hasher implementation
func NewHasher64(hf func() hash.Hash64) Hasher {
	return func(key, member []byte) uint64 {
		h := hf()
		h.Write(key)
		h.Write(member)
		return h.Sum64()
	}
}

// DefaultHasher is the default hash implementation, which uses a FNV-1 64a hasher under the covers.
func DefaultHasher(key, member []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	h.Write(member)
	return h.Sum64()
}

// Entry is a tuple containing the member object together with its hash value
type Entry struct {
	// Member is the object returned by the rendezvous hash for a given key
	Member interface{}

	// Value is the hash value of the member
	Value []byte
}

// Hash implements a rendezvous hash over a set of members.  A Hash instance
// is safe for concurrent reads and writes.  It is immutable once created by a Builder.
//
// Hash values are created by concatenating key bytes and member bytes, then computing
// the hash based on that single byte slice.
type Hash struct {
	entries []Entry
	hasher  Hasher
}

// Len returns the number of entries in the rendezvous hash table.  If this method
// returns 0, all methods that return members will return nil.
func (h *Hash) Len() int {
	return len(h.entries)
}

// Get returns the result of a rendezvous hash given an arbitrary key
func (h *Hash) Get(key []byte) interface{} {
	if len(h.entries) == 0 {
		return nil
	}

	return h.get(key)
}

// GetString returns the result of a rendezvous hash using a string key
func (h *Hash) GetString(key string) interface{} {
	if len(h.entries) == 0 {
		// be kind to the gc: avoid an extra byte slice if we're empty anyway
		return nil
	}

	return h.get([]byte(key))
}

func (h *Hash) get(key []byte) interface{} {
	var (
		champion interface{}
		value    uint64
	)

	for _, e := range h.entries {
		if v := h.hasher(key, e.Value); v > value {
			champion = e.Member
			value = v
		}
	}

	return champion
}

var emptyHash = Hash{hasher: DefaultHasher}

// EmptyHash returns the canonicalized empty Hash instance.  This is used mainly by
// the builder when no entries have been added.
func EmptyHash() *Hash {
	return &emptyHash
}

// Builder is a mutable, fluent builder for Hash instances.  Builders are not safe
// for concurrent reads and writes.  The zero value for this struct is a valid instance.
type Builder struct {
	entries []Entry
	hasher  Hasher
}

// Hasher sets the Hasher strategy for the next Hash created by this builder.
// By default, DefaultHasher is used.
func (b *Builder) Hasher(h Hasher) *Builder {
	b.hasher = h
	return b
}

// Hash32 uses a 32-bit hashing constructor as the hash algorithm
func (b *Builder) Hash32(hf func() hash.Hash32) *Builder {
	return b.Hasher(NewHasher32(hf))
}

// Hash64 uses a 64-bit hashing constructor as the hash algorithm
func (b *Builder) Hash64(hf func() hash.Hash64) *Builder {
	return b.Hasher(NewHasher64(hf))
}

// Add appends entries to the final Hash
func (b *Builder) Add(e ...Entry) *Builder {
	b.entries = append(b.entries, e...)
	return b
}

// AddMember appends a single member together with its hash value
func (b *Builder) AddMember(m interface{}, v []byte) *Builder {
	return b.Add(Entry{Member: m, Value: v})
}

// AddStrings adds several string members, where each member's hash value
// is simply the byte slice representation of the string.
func (b *Builder) AddStrings(ms ...string) *Builder {
	for _, m := range ms {
		b.Add(Entry{Member: m, Value: []byte(m)})
	}

	return b
}

// New creates a Hash using this Builder's current configuration.  This builder
// is reset prior to returning.
func (b *Builder) New() *Hash {
	if len(b.entries) == 0 {
		return EmptyHash()
	}

	h := &Hash{
		entries: b.entries,
		hasher:  b.hasher,
	}

	if h.hasher == nil {
		h.hasher = DefaultHasher
	}

	b.entries = nil
	b.hasher = nil
	return h
}
