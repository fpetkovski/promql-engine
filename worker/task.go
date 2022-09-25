package worker

import "sync"

type Task struct {
	mu     *sync.Mutex
	action func()
}

func NewTask(action func()) *Task {
	mu := &sync.Mutex{}
	mu.Lock()
	return &Task{
		mu:     mu,
		action: action,
	}
}

func (t *Task) Wait() {
	t.mu.Lock()
}

func (t *Task) markDone() {
	t.mu.Unlock()
}
