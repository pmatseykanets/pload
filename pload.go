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
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func read(done <-chan struct{}, reader *csv.Reader, config config) (<-chan []string, <-chan error) {
	records := make(chan []string, config.Workers)
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

func nullify(value string) interface{} {
	if value == "null" {
		return sql.NullString{}
	}

	return value
}

func nullifyImportId(importId int) interface{} {
	if importId == 0 {
		return sql.NullString{}
	}

	return importId
}

type ingestResult struct {
	Processed int
	Affected  int
}

func buildQuery(table string, n int) string {
	sql :=
		`WITH inserted AS (
		INSERT INTO %s (
			_dw_last_import_id, marketoguid, leadid, activitydate, activitytypeid,
			campaignid, primaryattributevalueid, primaryattributevalue, attributes
		) VALUES %s
		ON CONFLICT (marketoguid) DO NOTHING
		RETURNING 1
	)
	SELECT COUNT(*) FROM inserted`

	v := make([]string, n)
	p := make([]string, 9)
	m := 0
	for i := 0; i < n; i++ {
		for j := 0; j < 9; j++ {
			m++
			p[j] = fmt.Sprintf("$%d", m)
		}
		v[i] = fmt.Sprintf("(%s)", strings.Join(p, ","))
	}

	return fmt.Sprintf(sql, table, strings.Join(v, ","))
}

func ingest(db *sql.DB, config config, done <-chan struct{}, records <-chan []string, results chan<- ingestResult) {
	txCount := 0
	inCount := 0
	inAffected := 0
	processed := 0
	affected := 0
	importId := nullifyImportId(config.ImportId)

	bindings := make([]interface{}, config.InsertSize*9)

	// Build the query that will be used in a loop
	query := buildQuery(config.Table, config.InsertSize)
	// Open a transaction and prepare the statement
	tx, err := db.Begin()
	stmt, err := tx.Prepare(query)
	if err != nil {
		log.Fatal(err)
	}

	for record := range records {
		// If we reached the TxSize number of affected records
		// commit the transaction, reset the counter and immediately open a new one
		if txCount >= config.TxSize {
			stmt.Close()
			err := tx.Commit()
			if err != nil {
				log.Fatal(err)
			}

			txCount = 0
			tx, err = db.Begin()
			stmt, err = tx.Prepare(query)
			if err != nil {
				log.Fatal(err)
			}
		}

		// If we accumulated InserSize number of records
		// perform the multi-row insert and reset the counter
		if inCount >= config.InsertSize {
			err := stmt.QueryRow(bindings...).Scan(&inAffected)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				// TODO: Revisit and communicate the error via an error channel
				log.Fatal(err)
			}
			affected += inAffected
			processed += config.InsertSize
			inCount = 0
		}

		// Accumulate bindings for the insert query
		bindings[inCount*9] = importId
		for i, value := range record {
			bindings[inCount*9+i+1] = nullify(value)
		}
		inCount++
	}
	// Close the prepared statement
	stmt.Close()

	// If there are left over records
	// adjust the query accordingly and perform the insert
	if inCount > 0 {
		query = buildQuery(config.Table, inCount)
		err := tx.QueryRow(query, bindings[0:inCount*9]...).Scan(&inAffected)
		if err != nil {
			tx.Rollback()
			// TODO: Revisit and communicate the error via an error channel
			log.Fatal(err)
		}
		affected += inAffected
		processed += inCount
	}

	// Commit the very last transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	select {
	case results <- ingestResult{processed, affected}:
	case <-done:
		return
	}
}

func ingestAll(reader *csv.Reader, db *sql.DB, config config) (ingestResult, error) {
	done := make(chan struct{})
	defer close(done)

	// Read and discard the header
	reader.Read()

	// Errors channel
	records, errc := read(done, reader, config)

	// Start a fixed number of ingest workers
	results := make(chan ingestResult)

	var wg sync.WaitGroup

	wg.Add(config.Workers)
	for i := 0; i < config.Workers; i++ {
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
	totals := ingestResult{0, 0}

	for result := range results {
		totals.Processed += result.Processed
		totals.Affected += result.Affected
	}
	// Check whether the ingest failed
	if err := <-errc; err != nil {
		return ingestResult{0, 0}, err
	}

	return totals, nil
}

func memoryUsage() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return m.Sys
}

type config struct {
	ImportId   int
	Table      string
	Workers    int
	InsertSize int
	TxSize     int
}

type totals struct {
	Records  ingestResult
	Duration time.Duration
	Memory   uint64
}

func printTotals(totals *totals) {
	fmt.Printf(
		"Total %d, affected %d, time %v, memory %.3fMb\n",
		totals.Records.Processed,
		totals.Records.Affected,
		totals.Duration,
		float64(totals.Memory)/1024/1024,
	)
}

func printTotalsJSON(totals *totals) {
	json, _ := json.MarshalIndent(totals, "", "   ")
	fmt.Printf("%s\n", json)
}

func main() {
	var (
		dbConn     string
		config     config
		maxProcs   int
		totals     totals
		outputJSON bool
		reader     *csv.Reader
		baseReader *bufio.Reader
	)

	flag.StringVar(&dbConn, "c", "", "Database connection string")
	flag.IntVar(&config.Workers, "w", 4, "Number of workers")
	flag.IntVar(&config.ImportId, "i", 0, "Import Id")
	flag.StringVar(&config.Table, "t", "marketo.activities", "Database table to load data into")
	flag.IntVar(&maxProcs, "p", 1, "Max logical processors")
	flag.BoolVar(&outputJSON, "json", false, "Output results in JSON")
	flag.IntVar(&config.InsertSize, "m", 2, "Number of records per insert")
	flag.IntVar(&config.TxSize, "x", 25000, "Number of records per transaction")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] [file]\n", filepath.Base(os.Args[0]))
		fmt.Println("  file")
		fmt.Println("    	A CSV file to load. If omitted read from stdin")
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

	results, err := ingestAll(reader, db, config)
	if err != nil {
		log.Fatal(err)
		return
	}

	totals.Records = results
	totals.Duration = time.Since(start)
	totals.Memory = memoryUsage()

	if outputJSON {
		printTotalsJSON(&totals)
	} else {
		printTotals(&totals)
	}
}
