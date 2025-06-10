package memory

import (
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/valueobject"
	"sync"
)

type InMemoryQueue struct {
	messagesPerQueueName map[string][]valueobject.Message
	defaultQueueCapacity int
	mu                   sync.Mutex
}

func NewInMemoryQueue(startLen, defaultQueueCapacity int) *InMemoryQueue {
	return &InMemoryQueue{
		messagesPerQueueName: make(map[string][]valueobject.Message, startLen),
		defaultQueueCapacity: defaultQueueCapacity,
	}
}

func (r *InMemoryQueue) PutMessageToEnd(queueName string, message valueobject.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.messagesPerQueueName[queueName]; !ok {
		r.messagesPerQueueName[queueName] = make([]valueobject.Message, 0, r.defaultQueueCapacity)
	}

	r.messagesPerQueueName[queueName] = append(r.messagesPerQueueName[queueName], message)

	return nil
}

func (r *InMemoryQueue) GetFirstMessage(queueName string) (valueobject.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	messageList, isExist := r.messagesPerQueueName[queueName]
	if !isExist || len(messageList) == 0 {
		return valueobject.Message{}, model.ErrMessageNotFound
	}

	message := messageList[0]
	r.messagesPerQueueName[queueName] = messageList[1:]

	return message, nil
}

func (r *InMemoryQueue) CountMessages(queueName string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	messageList, isExist := r.messagesPerQueueName[queueName]
	if !isExist {
		return 0, nil
	}

	return len(messageList), nil
}
