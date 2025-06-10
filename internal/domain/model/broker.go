package model

import (
	"errors"
)

var ErrBrokerIsFull = errors.New("broker is full")
var ErrQueueNotFound = errors.New("queue not found")

type BrokerStorage interface {
	CreateQueue(name string) (*Queue, error)
	GetQueue(name string) (*Queue, error)
	CountQueues() (int, error)
}

type Broker struct {
	maxQueues int
	storage   BrokerStorage
}

func NewBroker(maxQueues int, repository BrokerStorage) *Broker {
	return &Broker{maxQueues: maxQueues, storage: repository}
}

func (b *Broker) GetQueue(queueName string) (*Queue, error) {
	return b.storage.GetQueue(queueName)
}

func (b *Broker) CreateQueue(queueName string) (*Queue, error) {
	isBrokerFull, err := b.isBrokerFull()
	if err != nil {
		return nil, err
	}

	if isBrokerFull {
		return nil, ErrBrokerIsFull
	}

	return b.storage.CreateQueue(queueName)
}

func (b *Broker) isBrokerFull() (bool, error) {
	if b.maxQueues == 0 {
		return false, nil
	}

	countQueues, err := b.storage.CountQueues()
	if err != nil {
		return true, err
	}

	if countQueues >= b.maxQueues {
		return true, nil
	}

	return false, nil
}
