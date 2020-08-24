package async

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

func ExecuteAsync(ctx context.Context, opts *Opts, chn chan *Response) error {
	tasksLen := len(opts.Tasks)
	if tasksLen == 0 {
		return nil
	}

	if opts.IsTaskUnordered {
		sortTasks(ctx, opts.Tasks)
	}

	var currIndex int
	var err error

	for currIndex < tasksLen {
		currIndex, err = launchTasks(ctx, opts, chn, currIndex)
		if err != nil {
			return err
		}
	}

	return nil
}

func launchTasks(ctx context.Context, opts *Opts, chn chan *Response, currIndex int) (int, error) {
	var taskLaunchedCount int32

	tasksLen := len(opts.Tasks)
	currPriority := opts.Tasks[currIndex].Priority

	startIndex := currIndex
	for currIndex < tasksLen && currPriority == opts.Tasks[currIndex].Priority {
		task := opts.Tasks[currIndex]
		newCtx, cancelFn := context.WithCancel(ctx)
		go task.Fn(newCtx, chn)
		defer cancelFn()
		taskLaunchedCount++
		currIndex++
	}

	fmt.Println("finish launch fns")
	startTime := time.Now()
	maxTime := startTime.Add(opts.Timeout)
	fmt.Println("time","starttime",startTime, "maxtime",maxTime)
	var fnDoneCount int32
	var err error
	for fnDoneCount < taskLaunchedCount && time.Now().Before(maxTime) {
		waitTime := time.Until(maxTime)
		fmt.Println("in a loop", "fnDoneCount", fnDoneCount, "remainingTime", waitTime)
		select {
		case rsp := <-chn:
			atomic.AddInt32(&fnDoneCount, 1)

			fmt.Println("response from fn", "rsp", rsp)
			if opts.Callback != nil {
				if callBackFn, ok := opts.Callback[rsp.JobName]; ok {
					callBackFn(ctx, rsp)
				}
			}

			fmt.Println("stop execute on error ", opts.StopOnError)
			if opts.StopOnError {
				err = rsp.Err
				return currIndex, err
			}

		case <-time.After(waitTime):
			fmt.Println("timeout wait for err response")

			return currIndex, fmt.Errorf("call extra info timeout [%d/%d]",
				startIndex+int(fnDoneCount), tasksLen)
		}
	}

	fmt.Println("done check loop", "err", err)

	return currIndex, nil
}

func sortTasks(ctx context.Context, tasks []Task) {
	sort.Sort(SortTaskByPriority(tasks))
}

func AsyncSendResponse(chn chan<- *Response, rsp *Response) {
	go func() {
		chn <- rsp
	}()
}
