package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = b.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := &BlockHashes{Hashes: blockHashesIn}
	b, err := c.MissingBlocks(ctx, in)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, server := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		//Handle errors appropriately
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			conn.Close()
			return err
		}

		*serverFileInfoMap = b.FileInfoMap

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, server := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.UpdateFile(ctx, fileMetaData)

		//Handle errors appropriately
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			conn.Close()
			return err
		}

		*latestVersion = b.Version

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, server := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		in := &BlockHashes{Hashes: blockHashesIn}
		b, err := c.GetBlockStoreMap(ctx, in)

		//Handle errors appropriately
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			conn.Close()
			return err
		}

		for sev, hash := range b.BlockStoreMap {
			(*blockStoreMap)[sev] = hash.Hashes
		}

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("could not find a leader")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, server := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		//Handle errors appropriately
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			conn.Close()
			return err
		}

		*blockStoreAddrs = b.BlockStoreAddrs

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("could not find a leader")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
