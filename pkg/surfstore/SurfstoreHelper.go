package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()

	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	stmt, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer stmt.Close()

	for file, meta := range fileMetas {
		for idx, hash := range meta.BlockHashList {
			_, err := stmt.Exec(file, meta.Version, idx, hash)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/

const (
	getDistinctFileName = `SELECT DISTINCT fileName FROM indexes;`
	getTuplesByFileName = `SELECT version, hashIndex, hashValue FROM indexes WHERE fileName=? ORDER BY hashIndex;`
)

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error When Querying Meta")
	}
	defer rows.Close()

	for rows.Next() {
		var fileName string
		if err := rows.Scan(&fileName); err != nil {
			log.Fatal("Error When Scanning Meta")
		}

		fileRows, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Fatal("Error When Querying File Meta")
		}
		defer fileRows.Close()

		metad := &FileMetaData{Filename: fileName}
		for fileRows.Next() {
			var version, hashIndex int
			var hashValue string
			if err := fileRows.Scan(&version, &hashIndex, &hashValue); err != nil {
				log.Fatal("Error When Scanning File Meta")
			}
			metad.Version = int32(version)
			metad.BlockHashList = append(metad.BlockHashList, hashValue)
		}
		fileMetaMap[fileName] = metad
	}

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
