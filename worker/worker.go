// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package worker

import (
	"context"
)

type Group struct {
	input   chan *Task
	workers []*Worker
}

func NewGroup(numWorkers int) *Group {
	input := make(chan *Task, 1024*numWorkers)
	workers := make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = newWorker(input)
	}
	return &Group{
		input:   input,
		workers: workers,
	}
}

func (g *Group) Start(ctx context.Context) {
	for _, w := range g.workers {
		go w.start(ctx)
	}
	go func() {
		select {
		case <-ctx.Done():
			close(g.input)
		}
	}()
}

func (g *Group) Send(task *Task) {
	g.input <- task
}

type Worker struct {
	input chan *Task
}

func newWorker(input chan *Task) *Worker {
	return &Worker{
		input: input,
	}
}

func (w *Worker) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-w.input:
			if ok {
				task.action()
				task.markDone()
			}
		}
	}
}
