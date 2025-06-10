package model

import (
	"errors"
	"go-test-task/internal/domain/valueobject"
)

var ErrQueueIsFull = errors.New("queue is full")
var ErrMessageNotFound = errors.New("message not found")

type QueueStorage interface {
	PutMessageToEnd(queueName string, message valueobject.Message) error
	GetFirstMessage(queueName string) (valueobject.Message, error)
	CountMessages(queueName string) (int, error)
}

type Queue struct {
	name        string
	maxMessages int
	storage     QueueStorage
}

func NewQueue(name string, maxMessages int, repository QueueStorage) *Queue {
	return &Queue{name: name, maxMessages: maxMessages, storage: repository}
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) GetMessage() (valueobject.Message, error) {
	return q.storage.GetFirstMessage(q.name)
}

func (q *Queue) PutMessage(message valueobject.Message) error {
	isQueueFull, err := q.isQueueFull()
	if err != nil {
		return err
	}

	if isQueueFull {
		return ErrQueueIsFull
	}

	return q.storage.PutMessageToEnd(q.name, message)
}

func (q *Queue) isQueueFull() (bool, error) {
	if q.maxMessages == 0 {
		return false, nil
	}

	countMessages, err := q.storage.CountMessages(q.name)
	if err != nil {
		return true, err
	}

	if countMessages >= q.maxMessages {
		return true, nil
	}

	return false, nil
}
