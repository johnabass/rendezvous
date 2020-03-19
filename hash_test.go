package rendezvous

import (
	"fmt"
	"hash/fnv"
)

func ExampleHash() {
	h := new(Builder).
		Hash32(fnv.New32a).
		AddStrings("foo.com", "bar.net").New()

	fmt.Println(h.GetString("mac:112233445566"))
	// Output: bar.net
}
