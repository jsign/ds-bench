package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ipfs/go-datastore"
	"github.com/kataras/tablewriter"
	badger "github.com/textileio/go-ds-badger3"
	mongods "github.com/textileio/go-ds-mongo"
)

var mongoURI = flag.String("mongo-uri", "mongodb://127.0.0.1:27017", "Mongo URI")

func main() {
	flag.Parse()
	bench()
}

func bench() {
	sizes := []int{500, 1 << 10, 10 << 10, 100 << 10}
	counts := []int{1, 10, 100, 500}
	withTxns := []bool{false, true}
	parallels := []int{1, 10, 100}

	for _, size := range sizes {
		for _, count := range counts {
			fmt.Printf("With item size %s with %d items\n", humanize.IBytes(uint64(size)), count)
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"parallel", "txn", "badger_duration_ms", "badger_errors", "mongo_duration_ms", "mongo_errors"})

			for _, withTxn := range withTxns {
				for _, parallel := range parallels {
					badgerDurationMs, badgerNumErrors := benchCase(NewBadgerTxnDatastore, size, count, parallel, withTxn)
					mongoDurationMs, mongoNumErrors := benchCase(NewMongoTxnDatastore, size, count, parallel, withTxn)
					r := []string{
						strconv.Itoa(parallel),
						strconv.FormatBool(withTxn),
						strconv.FormatInt(badgerDurationMs, 10),
						strconv.FormatInt(badgerNumErrors, 10),

						strconv.FormatInt(mongoDurationMs, 10),
						strconv.FormatInt(mongoNumErrors, 10),
					}
					table.Append(r)
				}
			}
			table.Render()
		}
	}

}

type dsFactory func() (datastore.TxnDatastore, func(), error)

func benchCase(createDS dsFactory, size, count, parallel int, withTxn bool) (int64, int64) {
	ds, clean, err := createDS()
	checkErr(err)
	defer clean()
	defer ds.Close()

	data := make([]byte, size)

	var wg sync.WaitGroup
	wg.Add(parallel)

	var totalDuration int64
	var numErrors int64
	for k := 0; k < parallel; k++ {
		go func() {
			defer wg.Done()
			w := ds.(datastore.Write)
			var txn datastore.Txn
			if withTxn {
				txn, err = ds.NewTransaction(false)
				if err != nil {
					panic(err)
				}
				defer txn.Discard()
				w = txn
			}
			start := time.Now()
			for i := 0; i < count; i++ {
				key := datastore.NewKey(fmt.Sprintf("%d", i))
				if err := w.Put(key, data); err != nil {
					fmt.Printf("put error: %s\n", err)
					atomic.AddInt64(&numErrors, 1)
				}
			}
			if withTxn {
				if err := txn.Commit(); err != nil {
					atomic.AddInt64(&numErrors, 1)
				}
			}
			atomic.AddInt64(&totalDuration, time.Since(start).Milliseconds())
		}()
	}

	wg.Wait()

	return atomic.LoadInt64(&totalDuration) / int64(parallel), numErrors
}

// NewBadgerTxnDatastore returns a new txndswrap.TxnDatastore backed by Badger.
func NewBadgerTxnDatastore() (datastore.TxnDatastore, func(), error) {
	tmpDir, err := ioutil.TempDir("", "")
	checkErr(err)

	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return nil, nil, err
	}

	ds, err := badger.NewDatastore(tmpDir, &badger.DefaultOptions)
	checkErr(err)

	return ds, func() { defer os.RemoveAll(tmpDir) }, nil

}

// NewMongoTxnDatastore returns a new txndswrap.TxnDatastore backed by MongoDB.
func NewMongoTxnDatastore() (datastore.TxnDatastore, func(), error) {
	mongoCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if *mongoURI == "" {
		return nil, nil, fmt.Errorf("mongo uri is empty")
	}
	ds, err := mongods.New(mongoCtx, *mongoURI, "ds-bench")
	if err != nil {
		return nil, nil, fmt.Errorf("opening mongo datastore: %s", err)
	}

	return ds, func() {}, nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
