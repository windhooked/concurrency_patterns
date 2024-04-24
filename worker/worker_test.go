package worker_test

import (
        "log"
        "sync"
        "testing"
        "time"

)

const (
        BatchingTimeout = 500 * time.Microsecond
        ReadTimeout     = 500 * time.Millisecond
        BatchSize       = 5
)

var testData = []string{
        "vBnvjMTw6jIQ",
        "HQu9Xeb4G705",
        "EmtSjR57mYmQ",
        "ETJozk8UL40E",
        "tOXUWFLtlPBY",
        "Fv8v0ac50Fw1",
        "87990046144C",
        "35014372479C",
        "70835131015C",
        "82632124111C",
        "24602564524C",
        "74148482090C",
        "31051620823C",
        "46505086131C",
        "49945906804C",
        "53942776956C",
        "98833020677C",
        "87298644139C",
        "33873353482C",
        "10392601398C",
        "70120890292C",
        "20171172309C",
        "22777845570C",
}

func TestWorkerPool(t *testing.T) {
        m := new(sync.Map)
        h := worker.New(2, 10, 500*time.Microsecond,
                func(in []interface{}) ([]interface{}, error) {
                        return in, nil
                },
                func(in []interface{}) error {
                        m.Store(in, in)
                        return nil
                },
                func(err error) {
                        log.Printf("%v", err)
                },
                func() bool { return false })

        h.Run()
}

// First test to implement the worker channels from first principles
// this test then became the worker class
func TestWorker(t *testing.T) {

        producer := func(data []string, requestChan chan interface{}) {
                for _, v := range testData {
                        requestChan <- v
                }
        }

        worker := func(jobChan chan interface{}, resultChan chan interface{}, errChan chan interface{}, doneChan chan bool) {
                //for j := range jobChan {
                for {
                        select {
                        case j := <-jobChan:
                                log.Printf("run job %v", j)
                                resultChan <- j
                        }
                }
                //      doneChan <- true
        }

        //emitter := func(jobChan chan interface{}, errChan chan interface{}) {
        collector := func(resultChan chan interface{}, errChan chan interface{}, doneChan chan bool) {
                count := 0
        DONE:
                for {
                        select {
                        case e := <-errChan:
                                log.Printf("%v", e)

                        case res := <-resultChan:
                                log.Printf("collecitng result job %v", res)
                        case <-doneChan:
                                count++
                                if count > 2 {
                                        break DONE
                                }
                        }
                }
        }

        batcher := func(requestChan chan interface{}, jobChan chan interface{}, errChan chan interface{}) {
                readBatch := make([]string, 0, BatchSize)
                batchingTimer := time.NewTimer(BatchingTimeout)
                batchingTimer.Stop()
                //DONE:
                for {
                        select {
                        case request, moreEntries := <-requestChan:
                                req := request.(string)
                                if !moreEntries {
                                        //break DONE
                                }
                                if len(readBatch) == 0 { // first entry in batch
                                        batchingTimer.Reset(BatchingTimeout)
                                }
                                readBatch = append(readBatch, req)
                                if len(readBatch) == BatchSize {
                                        batchingTimer.Stop()
                                        jobChan <- readBatch
                                        readBatch = make([]string, 0, BatchSize)
                                }
                        case <-batchingTimer.C:
                                if len(readBatch) != 0 {
                                        jobChan <- readBatch
                                        readBatch = make([]string, 0, BatchSize)
                                }
                        }
                }
                //close(jobChan)
        }

        //pool := pond.New(5, 0, pond.MinWorkers(5))
        requestChan := make(chan interface{})
        jobChan := make(chan interface{})
        resultChan := make(chan interface{})
        errChan := make(chan interface{})
        doneChan := make(chan bool)
        go batcher(requestChan, jobChan, errChan)
        go worker(jobChan, resultChan, errChan, doneChan)
        go worker(jobChan, resultChan, errChan, doneChan)
        go producer(testData, requestChan)

        collector(resultChan, errChan, doneChan)
}

