package processor

import (
        "context"
        "fmt"
        "log"
        "sync"
        "github.com/reactivex/rxgo/v2"
)

type (
        batchService struct {
                poolSize int
                BatchJobs *sync.Map
        }
)

func NewBatchService() *batchService {
        return &batchService{
                poolSize:       5,
                BatchJobs:      new(sync.Map),
        }
}

func (h *batchService) AddJob(j *job) {
        h.BatchJobs.Store(j.Key, j)
}

func (h *batchService) Process() {
        ch := make(chan rxgo.Item)

        //producer
        go func() {
                h.BatchJobs.Range(func(key, value any) bool {
                        v := value.(*job)
                        ch <- rxgo.Of(v)
                        return true
                })
                //ch <- rxgo.Error(errors.New("unknown"))
                close(ch)
        }()

        observable := rxgo.FromChannel(ch).
                Map(func(_ context.Context, value interface{}) (interface{}, error) {
                        j := value.(*job)
                        canProcess, haveErrors, err := j.ValidateJob()
                        if err != nil {
                                //handle me
                                j.Error = err
                                log.Printf("%v", err)
                                return j, nil
                        }
                        if len(haveErrors) > 0 {
                                j.TransactionsWithErrors = haveErrors
                        }

                        // now submit the "new"
                        if len(canProcess) > 0 {
                                j.Transactions = canProcess
                                result, err := j.ProcessJob()
                                if err != nil {
                                        j.Error = err
                                        return j, nil
                                }

                                j.Transactions = result
                        }
                        return j, nil
                }, rxgo.WithPool(h.poolSize))

        //now consume the results
        for item := range observable.Observe() {
                v := item.V.(*job)
                if item.Error() {
                        log.Printf("%v", item.E)
                }
                h.BatchJobs.Store(v.Key, v)
        }
}
