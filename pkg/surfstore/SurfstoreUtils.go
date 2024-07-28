package surfstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func ClientSync(client RPCClient) {
	baseDir := client.BaseDir
	blockSize := client.BlockSize

	localind, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		fmt.Println("Error reading local index:", err)
		return
	}

	localfile := make(map[string][]string)
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			var hashlist []string
			buf := make([]byte, blockSize)
			for {
				n, err := file.Read(buf)
				if err != nil && err.Error() != "EOF" {
					return err
				}
				if n == 0 {
					break
				}
				blockData := buf[:n]
				hash := GetBlockHashString(blockData)
				hashlist = append(hashlist, hash)
			}

			path := strings.TrimPrefix(path, baseDir+"/")
			localfile[path] = hashlist
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error scanning base directory:", err)
		return
	}

	newfile := make(map[string][]string)
	for file, hashl := range localfile {
		localf, exists := localind[file]
		if !exists || func(a, b []string) bool {
			if len(a) != len(b) {
				return false
			}
			for i := range a {
				if a[i] != b[i] {
					return false
				}
			}
			return true
		}(localf.BlockHashList, hashl) {
			newfile[file] = hashl
		}
	}

	remoteind := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteind)
	if err != nil {
		fmt.Println("Error fetching remote index:", err)
		return
	}

	for file, remoteFileInfo := range remoteind {
		_, exists := localind[file]
		if !exists {
			err := download(client, file, remoteFileInfo, baseDir)
			if err != nil {
				fmt.Println("Error downloading and reconstituting file:", err)
				return
			}
			localind[file] = remoteFileInfo
		}
	}

	for file, hashList := range newfile {
		localf, exists := localind[file]
		if !exists {
			err := upload(client, file, hashList, baseDir, blockSize)
			if err != nil {
				fmt.Println("Error uploading new file:", err)
				return
			}
			localind[file] = &FileMetaData{Filename: file, Version: 1, BlockHashList: hashList}
		} else {
			newVersion := localf.Version + 1
			fileMetaData := &FileMetaData{Filename: file, Version: newVersion, BlockHashList: hashList}
			var latestVersion int32
			err := client.UpdateFile(fileMetaData, &latestVersion)
			if err != nil {
				fmt.Println("Error updating modified file:", err)
				return
			}
			localind[file] = &FileMetaData{Filename: file, Version: newVersion, BlockHashList: hashList}
		}
	}

	err = WriteMetaFile(localind, baseDir)
	if err != nil {
		fmt.Println("Error writing local index:", err)
		return
	}
}

func download(client RPCClient, file string, remoteFileInfo *FileMetaData, baseDir string) error {
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap(remoteFileInfo.BlockHashList, &blockStoreMap)
	if err != nil {
		return err
	}

	filePath := filepath.Join(baseDir, file)
	fileData := []byte{}

	for addr, blockHashes := range blockStoreMap {
		for _, blockHash := range blockHashes {
			block := &Block{}
			err := client.GetBlock(blockHash, addr, block)
			if err != nil {
				return err
			}
			fileData = append(fileData, block.BlockData...)
		}
	}

	err = ioutil.WriteFile(filePath, fileData, 0644)
	if err != nil {
		return err
	}

	return nil
}

func upload(client RPCClient, file string, hashList []string, baseDir string, blockSize int) error {
	blockStoreMap := make(map[string][]string)
	err := client.GetBlockStoreMap(hashList, &blockStoreMap)
	if err != nil {
		return err
	}

	filePath := filepath.Join(baseDir, file)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, blockSize)
	for addr, _ := range blockStoreMap {
		for {
			n, err := f.Read(buf)
			if err != nil && err.Error() != "EOF" {
				return err
			}
			if n == 0 {
				break
			}
			blockData := buf[:n]
			block := &Block{BlockData: blockData, BlockSize: int32(n)}
			var succ bool
			err = client.PutBlock(block, addr, &succ)
			if err != nil {
				return err
			}
		}
	}

	fileMetaData := &FileMetaData{Filename: file, Version: 1, BlockHashList: hashList}
	var latestVersion int32
	err = client.UpdateFile(fileMetaData, &latestVersion)
	if err != nil {
		return err
	}

	return nil
}
