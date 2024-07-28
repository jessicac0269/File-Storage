package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hash := make([]string, 0, len(c.ServerMap))
	for k := range c.ServerMap {
		hash = append(hash, k)
	}
	sort.Strings(hash)

	idx := sort.Search(len(hash), func(i int) bool {
		return hash[i] > blockId
	})

	if idx == len(hash) {
		idx = 0
	}

	return c.ServerMap[hash[idx]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	servm := make(map[string]string)

	for _, addr := range serverAddrs {
		hash := (ConsistentHashRing{}).Hash("blockstore" + addr)
		servm[hash] = addr
	}

	return &ConsistentHashRing{
		ServerMap: servm,
	}
}
