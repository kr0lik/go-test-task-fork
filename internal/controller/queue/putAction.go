package queue

import (
	"encoding/json"
	"errors"
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/usecase"
	"go-test-task/internal/domain/valueobject"
	"go-test-task/internal/transport"
	"net/http"
)

type PutAction struct {
	putter *usecase.MessagePutter
}

func NewPutAction(putter *usecase.MessagePutter) *PutAction {
	return &PutAction{
		putter: putter,
	}
}

func (a *PutAction) Route() string {
	return "/queue/{queueName}"
}

func (a *PutAction) Method() string {
	return http.MethodPut
}

func (a *PutAction) Handle(w http.ResponseWriter, r *http.Request, params transport.Params) {
	queueName, isExist := params["queueName"]
	if !isExist || queueName == "" {
		http.Error(w, "invalid queue name", http.StatusBadRequest)

		return
	}

	var message valueobject.Message
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil || !message.IsValid() {
		http.Error(w, "invalid message", http.StatusBadRequest)

		return
	}

	if err := a.putter.Put(queueName, message); err != nil {
		switch {
		case errors.Is(err, model.ErrQueueIsFull),
			errors.Is(err, model.ErrBrokerIsFull):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}
