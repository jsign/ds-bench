package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/kataras/tablewriter"
	badger "github.com/textileio/go-ds-badger3"
	mongods "github.com/textileio/go-ds-mongo"
)

func main() {
	bench()
}

func bench() {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"size", "count", "parallel", "txn"})

	sizesExp := []int{1, 10, 20}
	counts := []int{1, 2, 4, 10}
	withTxns := []bool{false, true}
	parallels := []int{1}
	//sizesExp := []int{1, 10, 20}
	//	counts := []int{1, 2, 4, 10, 50, 100}
	//	withTxns := []bool{false, true}
	//	parallels := []int{1, 2, 5, 10, 50, 100}

	for _, sizeExp := range sizesExp {
		for _, count := range counts {
			for _, parallel := range parallels {
				for _, withTxn := range withTxns {
					durationMs, numErrors := benchCase(sizeExp, count, parallel, withTxn)
					r := []string{
						strconv.Itoa(1 << sizeExp),
						strconv.Itoa(count),
						strconv.Itoa(parallel),
						strconv.FormatBool(withTxn),
						strconv.FormatInt(durationMs, 10),
						strconv.FormatInt(numErrors, 10),
					}
					table.Append(r)
				}
			}
		}
	}

	table.Render()
}

func benchCase(sizeExp, count, parallel int, withTxn bool) (int64, int64) {
	tmpDir, err := ioutil.TempDir("", "")
	checkErr(err)
	defer os.RemoveAll(tmpDir)
	ds, err := NewBadgerTxnDatastore(tmpDir)
	checkErr(err)

	data := make([]byte, 1<<sizeExp)

	var wg sync.WaitGroup
	wg.Add(parallel)

	var totalDuration int64
	var numErrors int64
	for k := 0; k < parallel; k++ {
		k := k
		go func() {
			defer wg.Done()
			w := ds.(datastore.Write)
			if withTxn {
				txn, err := ds.NewTransaction(false)
				if err != nil {
					panic(err)
				}
				w = txn
			}
			start := time.Now()
			for i := 0; i < count; i++ {
				key := datastore.NewKey(fmt.Sprintf("%d_%d", k, i))
				if err := w.Put(key, data); err != nil {
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
func NewBadgerTxnDatastore(repoPath string) (datastore.TxnDatastore, error) {
	if err := os.MkdirAll(repoPath, os.ModePerm); err != nil {
		return nil, err
	}
	return badger.NewDatastore(repoPath, &badger.DefaultOptions)
}

// NewMongoTxnDatastore returns a new txndswrap.TxnDatastore backed by MongoDB.
func NewMongoTxnDatastore(uri, dbName string) (datastore.TxnDatastore, error) {
	mongoCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if uri == "" {
		return nil, fmt.Errorf("mongo uri is empty")
	}
	if dbName == "" {
		return nil, fmt.Errorf("mongo database name is empty")
	}
	ds, err := mongods.New(mongoCtx, uri, dbName)
	if err != nil {
		return nil, fmt.Errorf("opening mongo datastore: %s", err)
	}

	return ds, nil
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
