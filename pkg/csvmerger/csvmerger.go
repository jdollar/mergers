package csvmerger

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/google/uuid"
)

type duplicateActionsEnum struct {
	Combine int
	Prefix  int
}

var DuplicateActions = duplicateActionsEnum{
	Combine: 0,
	Prefix:  1,
}

type mergerReader struct {
	ID     uuid.UUID
	Reader *csv.Reader
}

type CsvMerger struct {
	DuplicateAction int
	Output          io.Writer
}

func grabAllHeaders(r []mergerReader) map[uuid.UUID][]string {
	headers := make(map[uuid.UUID][]string)

	for _, wrappedReader := range r {
		header, err := wrappedReader.Reader.Read()
		if err != nil {
			log.Fatal(err)
		}

		headers[wrappedReader.ID] = header
	}

	return headers
}

func wrapReaders(r []*csv.Reader) []mergerReader {
	var wrappedReaders []mergerReader
	for _, reader := range r {
		wrappedReaders = append(
			wrappedReaders,
			mergerReader{
				Reader: reader,
				ID:     uuid.Must(uuid.NewRandom()),
			},
		)
	}

	return wrappedReaders
}

func createMasterHeaderListAndDataMap(cm *CsvMerger, ro []uuid.UUID, m map[uuid.UUID][]string) ([]string, map[uuid.UUID][]int) {
	var masterHeaders []string
	dataMapping := make(map[uuid.UUID][]int)

	headerLocs := make(map[string]int)

	for _, ID := range ro {
		for _, header := range m[ID] {
			if loc, ok := headerLocs[header]; ok {
				switch cm.DuplicateAction {
				case DuplicateActions.Combine:
					dataMapping[ID] = append(dataMapping[ID], loc)
					break
				case DuplicateActions.Prefix:
					lastHeaderIdx := len(masterHeaders)
					masterHeaders = append(masterHeaders, uuid.Must(uuid.NewRandom()).String()+"_"+header)

					dataMapping[ID] = append(dataMapping[ID], lastHeaderIdx)
					break
				}
			} else {
				lastHeaderIdx := len(masterHeaders)
				masterHeaders = append(masterHeaders, header)

				headerLocs[header] = lastHeaderIdx
				dataMapping[ID] = append(dataMapping[ID], lastHeaderIdx)
			}
		}
	}

	return masterHeaders, dataMapping
}

func New() *CsvMerger {
	return &CsvMerger{
		DuplicateAction: DuplicateActions.Combine,
		Output:          os.Stdout,
	}
}

func (m *CsvMerger) Merge(r ...*csv.Reader) {
	fmt.Println(m)

	wrappedReaders := wrapReaders(r)

	headers := grabAllHeaders(wrappedReaders)
	fmt.Println(headers)

	var readerOrder []uuid.UUID
	for _, reader := range wrappedReaders {
		readerOrder = append(readerOrder, reader.ID)
	}

	masterHeaderList, csvColumnMappings := createMasterHeaderListAndDataMap(m, readerOrder, headers)

	fileWriter := csv.NewWriter(m.Output)
	fileWriter.Write(masterHeaderList)
	fileWriter.Flush()

	// Everything after here can be parallelized
	for _, wrappedReader := range wrappedReaders {
		columnMapping := csvColumnMappings[wrappedReader.ID]
		for {
			writeRecord := make([]string, len(masterHeaderList))
			readRecord, err := wrappedReader.Reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}

			for i, readVal := range readRecord {
				writeRecord[columnMapping[i]] = readVal
			}

			fileWriter.Write(writeRecord)
			fileWriter.Flush()
		}
	}
}
