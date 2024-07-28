package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	curr, exist := m.FileMetaMap[fileMetaData.Filename]

	if !exist {
		if fileMetaData.Version != 1 {
			return &Version{Version: -1}, errors.New("incorrect version")
		}
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}

	if fileMetaData.Version != curr.Version+1 {
		return &Version{Version: -1}, errors.New("mismatched version number")
	}

	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	bmap := make(map[string][]string)

	for _, hash := range blockHashesIn.Hashes {
		serv := m.ConsistentHashRing.GetResponsibleServer(hash)
		bmap[serv] = append(bmap[serv], hash)
	}

	bsmap := make(map[string]*BlockHashes)
	for serv, hashes := range bmap {
		bsmap[serv] = &BlockHashes{Hashes: hashes}
	}

	return &BlockStoreMap{BlockStoreMap: bsmap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
