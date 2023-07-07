package model

import "github.com/prometheus/prometheus/model/labels"

type LabelsIterator interface {
	Size() int
	Next() bool
	At() labels.Labels
	Err() error
}

type LabelsGetter interface {
	Labels() labels.Labels
}

type labelsGetterIterator[T LabelsGetter] struct {
	i       int
	getters []T
}

func NewLabelsGettersIterator[T LabelsGetter](getters []T) LabelsIterator {
	return &labelsGetterIterator[T]{getters: getters, i: -1}
}

func (l *labelsGetterIterator[T]) Size() int {
	return len(l.getters)
}

func (l *labelsGetterIterator[T]) Next() bool {
	l.i++
	return l.i < len(l.getters)
}

func (l *labelsGetterIterator[T]) At() labels.Labels {
	return l.getters[l.i].Labels()
}

func (l *labelsGetterIterator[T]) Err() error {
	return nil
}

type mergingIterator struct {
	its  []LabelsIterator
	size int
}

func NewMergingIterator(its []LabelsIterator) LabelsIterator {
	var size int
	for _, it := range its {
		size += it.Size()
	}
	return &mergingIterator{its: its, size: size}
}

func (m *mergingIterator) Size() int {
	return m.size
}

func (m *mergingIterator) Next() bool {
	if len(m.its) == 0 {
		return false
	}
	if m.its[0].Next() {
		return true
	}
	m.its = m.its[1:]
	return m.Next()
}

func (m *mergingIterator) At() labels.Labels {
	return m.its[0].At()
}

func (m *mergingIterator) Err() error {
	if len(m.its) == 0 {
		return nil
	}
	return m.its[0].Err()
}

type emptyLabels struct{}

func EmptyLabels() LabelsIterator {
	return &emptyLabels{}
}

func (e emptyLabels) Size() int { return 0 }

func (e emptyLabels) Next() bool { return false }

func (e emptyLabels) At() labels.Labels { return nil }

func (e emptyLabels) Err() error { return nil }

type processLabelsFunc func(labels.Labels) labels.Labels

type processingIterator struct {
	it      LabelsIterator
	process processLabelsFunc
}

func NewProcessingIterator(it LabelsIterator, process processLabelsFunc) LabelsIterator {
	return &processingIterator{it: it, process: process}
}

func (p processingIterator) Next() bool {
	return p.it.Next()
}

func (p processingIterator) At() labels.Labels {
	return p.process(p.it.At())
}

func (p processingIterator) Size() int { return p.it.Size() }

func (p processingIterator) Err() error {
	return p.it.Err()
}

type labelSliceIterator struct {
	idx   int
	items []labels.Labels
	err   error
}

func NewLabelSliceIterator(items []labels.Labels) LabelsIterator {
	return &labelSliceIterator{
		idx:   -1,
		items: items,
	}
}

func (l *labelSliceIterator) Next() bool {
	l.idx++
	return l.idx < len(l.items)
}

func (l *labelSliceIterator) At() labels.Labels {
	return l.items[l.idx]
}

func (l *labelSliceIterator) Err() error {
	return nil
}

func (l *labelSliceIterator) Size() int { return len(l.items) }

type labelsErr struct {
	err error
}

func ErrorLabels(err error) LabelsIterator {
	return &labelsErr{err: err}
}

func (l labelsErr) Next() bool { return false }

func (l labelsErr) At() labels.Labels { return nil }

func (l labelsErr) Err() error { return l.err }

func (l labelsErr) Size() int { return 0 }
