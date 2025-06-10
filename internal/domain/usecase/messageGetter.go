package usecase

import (
	"context"
	"errors"
	"fmt"
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/valueobject"
	"time"
)

type MessageGetter struct {
	broker *model.Broker
	waiter *model.Waiter
}

func NewMessageGetter(broker *model.Broker, waiter *model.Waiter) *MessageGetter {
	return &MessageGetter{broker: broker, waiter: waiter}
}

func (p *MessageGetter) Get(queueName string, waitTimeout time.Duration, ctx context.Context) (valueobject.Message, error) {
	const op = "MessageGetter.Get"

	var res valueobject.Message

	queue, err := p.broker.GetQueue(queueName)
	if err != nil {
		return res, fmt.Errorf("%s: %w", op, err)
	}

	message, err := queue.GetMessage()
	if err == nil {
		return message, nil
	}

	if !errors.Is(err, model.ErrMessageNotFound) {
		return res, fmt.Errorf("%s: %w", op, err)
	}

	message, err = p.waiter.WaitMessage(queueName, waitTimeout, ctx)
	if err != nil {
		return res, fmt.Errorf("%s: %w", op, err)
	}

	return message, nil
}
