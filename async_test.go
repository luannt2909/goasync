package async

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestExecuteAsyncSuccess(t *testing.T) {
	rspChn := make(chan *Response)
	syncObj := sync.Mutex{}
	result := make([]int, 0, 20)
	successFn := func(value int) ProcessTaskFn {
		return func(ctx context.Context, chn chan<- *Response) {
			syncObj.Lock()
			defer syncObj.Unlock()
			result = append(result, value)
			chn <- &Response{Err: nil}
		}
	}
	tasks := []Task{
		{
			Fn: successFn(1),
		},
		{
			Fn: successFn(2),
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Second,
		Tasks:   tasks,
	}, rspChn)
	assert.NoError(t, err)
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	assert.Equal(t, []int{1, 2}, result)
}

func TestExecuteAsyncWithPrioritySuccess(t *testing.T) {
	rspChn := make(chan *Response)
	syncObj := sync.Mutex{}
	result := make([]int, 0, 20)
	successFn := func(value int) ProcessTaskFn {
		return func(ctx context.Context, chn chan<- *Response) {
			syncObj.Lock()
			defer syncObj.Unlock()
			result = append(result, value)
			chn <- &Response{Err: nil}
		}
	}
	tasks := []Task{
		{Fn: successFn(0), Priority: 0}, {Fn: successFn(0), Priority: 0},
		{Fn: successFn(1), Priority: 1}, {Fn: successFn(1), Priority: 1},
		{Fn: successFn(1), Priority: 1}, {Fn: successFn(1), Priority: 1},
		{Fn: successFn(3), Priority: 3}, {Fn: successFn(3), Priority: 3},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Second,
		Tasks:   tasks,
	}, rspChn)
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 0, 1, 1, 1, 1, 3, 3}, result)
}

func failFn(ctx context.Context, chn chan<- *Response) {
	chn <- &Response{Err: errors.New("bad request blahblah")}
}

func TestExecuteAsyncWithPriorityFailed(t *testing.T) {
	rspChn := make(chan *Response)
	result := make([]int, 0, 20)
	startTime := time.Now()
	successFn := func(value int) ProcessTaskFn {
		return func(ctx context.Context, chn chan<- *Response) {
			result = append(result, value)
			chn <- &Response{}
		}
	}
	tasks := []Task{
		{Fn: successFn(0), Priority: 0}, {Fn: successFn(1), Priority: 1},
		{Fn: successFn(2), Priority: 2}, {Fn: successFn(3), Priority: 3},
		{Fn: failFn, Priority: 4}, {Fn: successFn(5), Priority: 5},
		{Fn: successFn(6), Priority: 6}, {Fn: successFn(7), Priority: 7},
	}

	timeout := time.Second * 5
	err := ExecuteAsync(context.Background(), &Opts{
		StopOnError: true,
		Timeout:     timeout,
		Tasks:       tasks,
	}, rspChn)

	assert.Equal(t, "bad request blahblah", err.Error())
	assert.True(t, time.Now().Sub(startTime).Nanoseconds() < timeout.Nanoseconds())
	assert.Equal(t, []int{0, 1, 2, 3}, result)
}

func TestExecuteAsyncWithPriorityUnorderedSuccess(t *testing.T) {
	rspChn := make(chan *Response)
	syncObj := sync.Mutex{}
	result := make([]int, 0, 20)
	successFn := func(value int) ProcessTaskFn {
		return func(ctx context.Context, chn chan<- *Response) {
			syncObj.Lock()
			defer syncObj.Unlock()
			result = append(result, value)
			chn <- &Response{Err: nil}
		}
	}
	tasks := []Task{
		{Fn: successFn(0), Priority: 0}, {Fn: successFn(3), Priority: 3},
		{Fn: successFn(1), Priority: 1}, {Fn: successFn(0), Priority: 0},
		{Fn: successFn(1), Priority: 1}, {Fn: successFn(1), Priority: 1},
		{Fn: successFn(3), Priority: 3}, {Fn: successFn(1), Priority: 1},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout:         time.Second,
		Tasks:           tasks,
		IsTaskUnordered: true,
	}, rspChn)
	assert.NoError(t, err)
	assert.Equal(t, []int{0, 0, 1, 1, 1, 1, 3, 3}, result)
}

func TestExecuteAsyncWithPriorityUnorderedFailed(t *testing.T) {
	rspChn := make(chan *Response)
	startTime := time.Now()
	result := make([]int, 0, 20)
	successFn := func(value int) ProcessTaskFn {
		return func(ctx context.Context, chn chan<- *Response) {
			result = append(result, value)
			chn <- &Response{}
		}
	}
	tasks := []Task{
		{Fn: successFn(8), Priority: 8}, {Fn: successFn(7), Priority: 7},
		{Fn: successFn(2), Priority: 2}, {Fn: failFn, Priority: 4},
		{Fn: successFn(1), Priority: 1}, {Fn: successFn(5), Priority: 5},
		{Fn: successFn(9), Priority: 9}, {Fn: successFn(0), Priority: 0},
	}

	timeout := time.Second * 5
	err := ExecuteAsync(context.Background(), &Opts{
		Timeout:         timeout,
		Tasks:           tasks,
		IsTaskUnordered: true,
		StopOnError:     true,
	}, rspChn)
	assert.Error(t, err)
	assert.Equal(t, []int{0, 1, 2}, result)
	assert.Equal(t, "bad request blahblah", err.Error())
	assert.True(t, time.Now().Sub(startTime).Nanoseconds() < timeout.Nanoseconds())
}

func TestExecuteAsyncStopOnError(t *testing.T) {
	rspChn := make(chan *Response)
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				chn <- &Response{Err: nil}
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				chn <- &Response{Err: errors.New("bad request blahblah")}
			},
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout:     time.Second,
		StopOnError: true,
		Tasks:       tasks,
	}, rspChn)
	assert.Error(t, err)
	assert.Equal(t, "bad request blahblah", err.Error())
}

func TestExecuteAsyncPriorityStopOnError(t *testing.T) {
	rspChn := make(chan *Response)
	var taskCallCounted int32
	mutex := &sync.Mutex{}
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				chn <- &Response{Err: nil}
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				chn <- &Response{Err: errors.New("bad request")}
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				chn <- &Response{Err: errors.New("bad request")}
			},
			Priority: 2,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				chn <- &Response{Err: errors.New("bad request")}
			},
			Priority: 2,
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Second,
		Tasks:   tasks,
	}, rspChn)
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	assert.Equal(t, taskCallCounted, int32(4))
}

func TestExecuteAsyncErrorNotStop(t *testing.T) {
	rspChn := make(chan *Response)
	var taskCallCounted int32
	mutex := &sync.Mutex{}
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: errors.New("bad request")})
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Millisecond * 500,
		Tasks:   tasks,
	}, rspChn)
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	assert.Equal(t, taskCallCounted, int32(4))
}

func TestExecuteAsyncPriorityErrorStop(t *testing.T) {
	rspChn := make(chan *Response)
	var taskCallCounted int32
	mutex := &sync.Mutex{}
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: errors.New("bad request")})
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 2,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 2,
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout:     time.Millisecond * 500,
		StopOnError: true,
		Tasks:       tasks,
	}, rspChn)
	assert.Error(t, err)
	assert.Equal(t, "", err.Error())
	time.Sleep(50 * time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	assert.Equal(t, taskCallCounted, int32(2))
}

func TestExecuteAsyncPriorityErrorNotStop(t *testing.T) {
	rspChn := make(chan *Response)
	var taskCallCounted int32
	mutex := &sync.Mutex{}
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: errors.New("bad request")})
			},
			Priority: 1,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 2,
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				fn := inc(mutex, &taskCallCounted)
				defer fn()
				time.Sleep(10)
				AsyncSendResponse(chn, &Response{Err: nil})
			},
			Priority: 2,
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Millisecond * 500,
		Tasks:   tasks,
	}, rspChn)
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mutex.Lock()
	defer mutex.Unlock()
	assert.Equal(t, taskCallCounted, int32(4))
}

func TestExecuteAsyncTimeoutStop(t *testing.T) {
	rspChn := make(chan *Response)
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				time.Sleep(500 * time.Millisecond)
				chn <- &Response{}
			},
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Timeout: time.Millisecond * 10,
		Tasks:   tasks,
	}, rspChn)
	assert.Error(t, err)
	assert.Equal(t, "call extra info timeout [0/1]", err.Error())
}

func TestExecuteAsyncCallback(t *testing.T) {
	rspChn := make(chan *Response)
	tasks := []Task{
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				AsyncSendResponse(chn, &Response{
					JobName: "job1",
					Err:     nil,
				})
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				AsyncSendResponse(chn, &Response{
					JobName: "job2",
					Err:     nil,
				})
			},
		},
		{
			Fn: func(ctx context.Context, chn chan<- *Response) {
				AsyncSendResponse(chn, &Response{
					JobName: "job2",
					Err:     nil,
				})
			},
		},
	}
	callBackMapCount := sync.Map{}
	callBackMapCount.Store("job1", int32(0))
	callBackMapCount.Store("job2", int32(0))

	callbackFns := map[string]Callback{
		"job1": func(ctx context.Context, rsp *Response) {
			count, _ := callBackMapCount.Load(rsp.JobName)
			countInt32 := count.(int32)
			callBackMapCount.Store(rsp.JobName, countInt32+1)
		},
		"job2": func(ctx context.Context, rsp *Response) {
			count, _ := callBackMapCount.Load(rsp.JobName)
			countInt32 := count.(int32)
			callBackMapCount.Store(rsp.JobName, countInt32+1)
		},
	}

	err := ExecuteAsync(context.Background(), &Opts{
		Callback:    callbackFns,
		Timeout:     time.Second,
		StopOnError: true,
		Tasks:       tasks,
	}, rspChn)
	assert.NoError(t, err)
	expectedMapCount := map[string]int32{
		"job1": 1,
		"job2": 2,
	}
	for k, v := range expectedMapCount {
		val, ok := callBackMapCount.Load(k)
		assert.True(t, ok)
		valInt32 := val.(int32)
		assert.Equal(t, v, valInt32)
	}
}

func prepareFns() []Task {
	numOfFns := 100
	waitTime := 50 * time.Millisecond
	fn := func(ctx context.Context, chn chan<- *Response) {
		time.Sleep(waitTime)
		AsyncSendResponse(chn, &Response{})
	}
	tasks := make([]Task, numOfFns)
	for i := 0; i < numOfFns; i++ {
		tasks[i] = Task{
			Fn: fn,
		}
	}

	return tasks
}

func BenchmarkAsync(b *testing.B) {
	tasks := prepareFns()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		executeAsync(tasks)
	}
}

func BenchmarkSync(b *testing.B) {
	tasks := prepareFns()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		executeSync(tasks)
	}
}

func executeAsync(tasks []Task) {
	rspChn := make(chan *Response)
	_ = ExecuteAsync(context.Background(), &Opts{
		Timeout:     time.Second,
		StopOnError: true,
		Tasks:       tasks,
	}, rspChn)
}

func executeSync(tasks []Task) {
	rspChn := make(chan *Response)
	ctx := context.Background()
	for _, task := range tasks {
		task.Fn(ctx, rspChn)
	}
}

func inc(mutex *sync.Mutex, val *int32) func() {
	mutex.Lock()
	atomic.AddInt32(val, 1)
	return func() {
		mutex.Unlock()
	}
}
