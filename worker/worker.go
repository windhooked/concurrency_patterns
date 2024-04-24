package worker

import (
        "log"
        "time"
)

const (
        BatchingTimeout = 500 * time.Microsecond
        ReadTimeout     = 500 * time.Millisecond
        BatchSize       = 10
)

type workerPool struct {
        requestChan     chan interface{}
        jobChan         chan []interface{}
        resultChan      chan []interface{}
        errChan         chan interface{}
        isDoneHook      func() bool
        doneChan        chan bool
        processHook     func([]interface{}) ([]interface{}, error)
        collectHook     func([]interface{}) error
        errorHook       func(error)
        poolSize        int
        batchSize       int
        batchingTimeout time.Duration
}

func New(poolSize int, batchSize int, batchingTimeout time.Duration,
        processHook func([]interface{}) ([]interface{}, error),
        collectHook func([]interface{}) error,
        errorHook func(error),
        isDoneHook func() bool,
) *workerPool {
        return &workerPool{
                requestChan:     make(chan interface{}),
                jobChan:         make(chan []interface{}),
                resultChan:      make(chan []interface{}),
                errChan:         make(chan interface{}),
                doneChan:        make(chan bool),
                processHook:     processHook,
                collectHook:     collectHook,
                errorHook:       errorHook,
                isDoneHook:      isDoneHook,
                poolSize:        poolSize,
                batchSize:       batchSize,
                batchingTimeout: batchingTimeout,
        }

}

func (h *workerPool) Run() {
        go h.batcher()
        for n := 0; n < h.poolSize; n++ {
                go h.worker()
        }
        go func() {
                for {
                        select {
                        default:

                        }
                }
        }()

        h.collector()
}
func (h *workerPool) Stop() {
        h.doneChan <- true
}

func (h *workerPool) Submit(data interface{}) {
        h.requestChan <- data
}

func (h *workerPool) worker() {
        //for j := range jobChan {
        for {
                select {
                case <-h.doneChan:
                        return
                case j := <-h.jobChan:
                        if h.processHook != nil {
                                log.Printf("run job %v", j)
                                res, err := h.processHook(j)
                                if err != nil {
                                        h.errChan <- err
                                }
                                h.resultChan <- res
                                continue
                        }
                        h.resultChan <- j
                }
        }
}

func (h *workerPool) collector() {
        for {
                select {
                case e := <-h.errChan:
                        log.Printf("%v", e)
                        if h.errorHook != nil {
                                h.errorHook(e.(error))
                        }
                case res := <-h.resultChan:
                        if h.collectHook != nil {
                                log.Printf("collecitng result job %v", res)
                                err := h.collectHook(res)
                                if err != nil {
                                        h.errChan <- err
                                }
                        }
                case <-h.doneChan:
                        return
                }
        }
}

func (h *workerPool) batcher() {
        readBatch := make([]interface{}, 0, h.batchSize)
        batchingTimer := time.NewTimer(h.batchingTimeout)
        batchingTimer.Stop()
        for {
                select {
                case request, moreEntries := <-h.requestChan:
                        req := request
                        if !moreEntries {
                                return
                        }
                        if len(readBatch) == 0 { // first entry in batch
                                batchingTimer.Reset(BatchingTimeout)
                        }
                        readBatch = append(readBatch, req)
                        if len(readBatch) == BatchSize {
                                batchingTimer.Stop()
                                h.jobChan <- readBatch
                                readBatch = make([]interface{}, 0, h.batchSize)
                        }
                case <-batchingTimer.C:
                        if len(readBatch) != 0 {
                                h.jobChan <- readBatch
                                readBatch = make([]interface{}, 0, h.batchSize)
                        }
                case <-h.doneChan:
                        return
                }
        }
}
