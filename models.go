package async

import (
	"context"
	"time"
)

type Response struct {
	JobName string      `json:"jobname"`
	Result  interface{} `json:"result"`
	Err     error       `json:"err"`
}

type Opts struct {
	Tasks           []Task
	IsTaskUnordered bool
	Timeout         time.Duration
	StopOnError     bool
	Callback        map[string]Callback
}

type Task struct {
	Fn       ProcessTaskFn
	Priority int
}

type SortTaskByPriority []Task

func (a SortTaskByPriority) Len() int           { return len(a) }
func (a SortTaskByPriority) Less(i, j int) bool { return a[i].Priority < a[j].Priority }
func (a SortTaskByPriority) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type ProcessTaskFn func(ctx context.Context, rspChn chan<- *Response)
type Callback func(ctx context.Context, rsp *Response)
