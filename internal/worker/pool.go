// Package worker provides a bounded goroutine pool for concurrent task processing.
package worker

import (
	"context"
	"sync"
)

// Task represents a unit of work to be executed by the pool.
type Task func(ctx context.Context) error

// Pool manages a fixed number of worker goroutines.
type Pool struct {
	tasks   chan Task
	wg      sync.WaitGroup
	once    sync.Once
	cancel  context.CancelFunc
	ctx     context.Context
}

// New creates a new worker pool with the given concurrency level.
func New(concurrency int) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		tasks:  make(chan Task, concurrency*2),
		ctx:    ctx,
		cancel: cancel,
	}
	for i := 0; i < concurrency; i++ {
		p.wg.Add(1)
		go p.run()
	}
	return p
}

func (p *Pool) run() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			_ = task(p.ctx)
		case <-p.ctx.Done():
			return
		}
	}
}

// Submit enqueues a task for execution. Blocks if the queue is full.
func (p *Pool) Submit(task Task) {
	select {
	case p.tasks <- task:
	case <-p.ctx.Done():
	}
}

// Shutdown stops accepting new tasks and waits for running tasks to complete.
func (p *Pool) Shutdown() {
	p.once.Do(func() {
		close(p.tasks)
		p.wg.Wait()
		p.cancel()
	})
}
