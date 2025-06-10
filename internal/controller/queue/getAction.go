package queue

import (
	"encoding/json"
	"errors"
	"go-test-task/internal/domain/model"
	"go-test-task/internal/domain/usecase"
	"go-test-task/internal/transport"
	"net/http"
	"strconv"
	"time"
)

type GetAction struct {
	getter             *usecase.MessageGetter
	defaultWaitTimeout time.Duration
}

func NewGetAction(getter *usecase.MessageGetter, defaultTimeout time.Duration) *GetAction {
	return &GetAction{getter: getter, defaultWaitTimeout: defaultTimeout}
}

func (a *GetAction) Route() string {
	return "/queue/{queueName}"
}

func (a *GetAction) Method() string {
	return http.MethodGet
}

func (a *GetAction) Handle(w http.ResponseWriter, r *http.Request, params transport.Params) {
	queueName, isExist := params["queueName"]
	if !isExist || queueName == "" {
		http.Error(w, "invalid queue name", http.StatusBadRequest)

		return
	}

	waitTimeout := a.defaultWaitTimeout
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			waitTimeout = time.Duration(n) * time.Second
		}
	}

	message, err := a.getter.Get(queueName, waitTimeout, r.Context())
	if err != nil {
		switch {
		case errors.Is(err, model.ErrQueueNotFound),
			errors.Is(err, model.ErrWaitTimeout),
			errors.Is(err, model.ErrMessageNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(message)
}
