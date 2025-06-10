package usecase

import (
	"errors"
	"fmt"
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/valueobject"
)

type MessagePutter struct {
	broker *model.Broker
	waiter *model.Waiter
}

func NewMessagePutter(broker *model.Broker, waiter *model.Waiter) *MessagePutter {
	return &MessagePutter{broker: broker, waiter: waiter}
}

func (p *MessagePutter) Put(queueName string, message valueobject.Message) error {
	const op = "MessagePutter.Put"

	queue, err := p.getQueue(queueName)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if p.waiter.Notify(queue.Name(), message) {
		return nil
	}

	err = queue.PutMessage(message)
	if err != nil {
		return fmt.Errorf("%w: %s", err, op)
	}

	return nil
}

func (p *MessagePutter) getQueue(queueName string) (*model.Queue, error) {
	queue, err := p.broker.GetQueue(queueName)
	if err != nil {
		if !errors.Is(err, model.ErrQueueNotFound) {
			return nil, err
		}

		queue, err = p.broker.CreateQueue(queueName)
		if err != nil {
			return nil, err
		}
	}

	return queue, nil
}
