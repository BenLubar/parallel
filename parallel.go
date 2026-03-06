package parallel

import (
	"context"
	"runtime"
)

func Do[T any, R any](ctx context.Context, generateTask func(context.Context) (T, bool, error), runTask func(context.Context, T) (R, error), consumeResult func(context.Context, R) error, maxBacklog, workers int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}

	if maxBacklog <= 0 {
		maxBacklog = workers
	}

	type taskIndex struct {
		index int
		task  T
	}

	type taskResult struct {
		index  int
		result R
		err    error
		panic  any
	}

	inputIndex, outputIndex := 0, 0
	inputDone := false
	queue := make(chan taskIndex, maxBacklog)
	defer func() {
		if !inputDone {
			close(queue)
		}
	}()

	result := make(chan *taskResult, maxBacklog)
	backlog := make([]*taskResult, maxBacklog)

	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return

				case task, ok := <-queue:
					if !ok {
						return
					}

					func() {
						defer func() {
							if r := recover(); r != nil {
								result <- &taskResult{
									index: task.index,
									panic: r,
								}
							}
						}()

						r, err := runTask(ctx, task.task)
						result <- &taskResult{
							index:  task.index,
							result: r,
							err:    err,
						}
					}()
				}
			}
		}()
	}

	for inputIndex < outputIndex || !inputDone {
		if inputIndex < outputIndex+maxBacklog && !inputDone {
			task, done, err := generateTask(ctx)
			if err != nil {
				return err
			}

			if done {
				inputDone = true
				close(queue)
				continue
			}

			queue <- taskIndex{
				index: inputIndex,
				task:  task,
			}
			inputIndex++
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case r := <-result:
				if r.index < outputIndex {
					panic("internal error: index too low")
				}
				if r.index-outputIndex >= maxBacklog {
					panic("internal error: index too high")
				}
				if backlog[r.index-outputIndex] != nil {
					panic("internal error: duplicate result index")
				}

				backlog[r.index-outputIndex] = r

				continue

			default:
			}

			break
		}

		for backlog[0] != nil {
			if backlog[0].panic != nil {
				panic(backlog[0].panic)
			}
			if backlog[0].err != nil {
				return backlog[0].err
			}

			err := consumeResult(ctx, backlog[0].result)
			if err != nil {
				return err
			}

			copy(backlog, backlog[1:])
			backlog[maxBacklog-1] = nil
			outputIndex++
		}
	}

	return nil
}
