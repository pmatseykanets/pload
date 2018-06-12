package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func read(done <-chan struct{}, reader *csv.Reader, config config) (<-chan []string, <-chan error) {
	records := make(chan []string, config.workers)
	errc := make(chan error, 1)

	go func() {
		// Close records channel after reading finished
		defer close(records)

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errc <- err
				break
			}

			select {
			case records <- record:
			case <-done:
				errc <- errors.New("Cancelled")
			}
		}
		errc <- nil
	}()

	return records, errc
}

type result struct {
	line     int
	affected int
	err      error
}

func nullify(value string) interface{} {
	if value == "null" {
		return sql.NullString{}
	}

	return value
}

func nullifyImportId(importId int) string {
	if importId == 0 {
		return "NULL"
	}

	return fmt.Sprintf("%d", importId)
}

func ingest(db *sql.DB, config config, done <-chan struct{}, records <-chan []string, results chan<- result) {
	query := fmt.Sprintf(
		`INSERT INTO %s (
			_dw_last_import_id, marketoguid, leadid, activitydate, activitytypeid,
			campaignid, primaryattributevalueid, primaryattributevalue, attributes
		) VALUES (%s, $1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (marketoguid) DO NOTHING
		RETURNING _dw_id`,
		config.table,
		nullifyImportId(config.importId),
	)

	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for record := range records {
		nullified := make([]interface{}, 8)

		for i, value := range record {
			nullified[i] = nullify(value)
		}

		affected := 1
		lastId := 0
		err := stmt.QueryRow(nullified...).Scan(&lastId)
		if err == sql.ErrNoRows {
			affected = 0
			err = nil
		}

		select {
		case results <- result{0, affected, err}:
		case <-done:
			return
		}
	}
}

func ingestAll(reader *csv.Reader, db *sql.DB, config config) (int, error) {
	done := make(chan struct{})
	defer close(done)

	// Read and discard the header
	reader.Read()

	// Errors channel
	records, errc := read(done, reader, config)

	// Start a fixed number of ingest workers
	results := make(chan result)

	var wg sync.WaitGroup

	wg.Add(config.workers)
	for i := 0; i < config.workers; i++ {
		go func() {
			ingest(db, config, done, records, results)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	// Receive all the results from results channel then check the error from errc channel
	affected := 0

	for result := range results {
		if result.err != nil {
			return -1, result.err
		}
		affected += result.affected
	}
	// Check whether the ingest failed
	if err := <-errc; err != nil {
		return -1, err
	}

	return affected, nil
}

func memoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.TotalAlloc
}

type config struct {
	importId int
	table    string
	workers  int
}

type totals struct {
	TotalRecords    int
	AffectedRecords int
	Duration        time.Duration
	Memory          uint64
}

func printTotals(totals *totals) {
	fmt.Printf(
		"Total %d records, time %v, memory %.3fMb\n",
		totals.AffectedRecords,
		totals.Duration,
		float64(totals.Memory)/1024/1024,
	)
}

func printTotalsJSON(totals *totals) {
	// fmt.Println(totals)
	json, _ := json.MarshalIndent(totals, "", "   ")
	fmt.Printf("%s\n", json)
}

func main() {
	var (
		dbConn     string
		config     config
		maxProcs   int
		totals     totals
		outJSON    bool
		reader     *csv.Reader
		baseReader *bufio.Reader
	)

	flag.StringVar(&dbConn, "c", "", "Database connection string")
	flag.IntVar(&config.workers, "w", 4, "Number of workers")
	flag.IntVar(&config.importId, "i", 0, "Import Id")
	flag.StringVar(&config.table, "t", "marketo.activities", "Database table")
	flag.IntVar(&maxProcs, "p", 1, "Max logical processors")
	flag.BoolVar(&outJSON, "j", false, "Output totals in JSON")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] [file]\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	// Set the number of logical processors to use
	runtime.GOMAXPROCS(maxProcs)

	// Start timing
	start := time.Now()

	if flag.NArg() < 1 {
		baseReader = bufio.NewReader(os.Stdin)
	} else {
		path := flag.Args()[0]
		file, err := os.Open(path)
		if err != nil {
			log.Fatalf("Can't open input file '%s'", path)
		}
		defer file.Close()

		baseReader = bufio.NewReader(file)
	}

	// Read magic bytes in hope to detect gzip
	bytes, err := baseReader.Peek(2)
	if err != nil {
		log.Fatal(err)
	}

	// The RFC 1952: GZIP file format specification version 4.3
	// states the first 2 bytes of the file are '\x1F' and '\x8B'.
	if bytes[0] == 0x1f && bytes[1] == 0x8b {
		gzipReader, err := gzip.NewReader(baseReader)
		if err != nil {
			log.Fatal(err)
		}
		reader = csv.NewReader(gzipReader)
	} else {
		reader = csv.NewReader(baseReader)
	}

	db, err := sql.Open("postgres", dbConn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	affected, err := ingestAll(reader, db, config)
	if err != nil {
		log.Fatal(err)
		return
	}

	totals.AffectedRecords = affected
	totals.Duration = time.Since(start)
	totals.Memory = memoryUsage()

	if outJSON {
		printTotalsJSON(&totals)
	} else {
		printTotals(&totals)
	}
}
