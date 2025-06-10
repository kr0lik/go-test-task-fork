package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go-test-task/internal/domain/valueobject"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func Test_Put_Then_Get_Message(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	// 1. PUT message
	msg := valueobject.Message{Content: "test message"}
	body, _ := json.Marshal(msg)
	req := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)

	// 2. GET message
	req2 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)

	assert.Equal(t, http.StatusOK, resp2.Code)

	var result valueobject.Message
	err := json.NewDecoder(resp2.Body).Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, msg.Content, result.Content)
}

func Test_Get_Waiters_ReceiveMessagesInOrder(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	// 1. make empty queue
	msg := valueobject.Message{Content: "test message"}
	body, _ := json.Marshal(msg)
	req := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)

	// 2. clear queue
	req2 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)

	// Run test

	messageCount := 3
	results := make([]string, messageCount)

	// 1. Add waiters
	for i := range messageCount {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
			resp := httptest.NewRecorder()
			httpHandler.ServeHTTP(resp, req)
			assert.Equal(t, http.StatusOK, resp.Code)

			var msg valueobject.Message
			json.NewDecoder(resp.Body).Decode(&msg)
			results[i] = msg.Content
		}()

		time.Sleep(100 * time.Millisecond)
	}

	// 3. Put messages
	for i := range messageCount {
		msg := valueobject.Message{Content: fmt.Sprintf("msg-%d", i)}
		body, _ := json.Marshal(msg)
		req := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body))
		resp := httptest.NewRecorder()
		httpHandler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)
	}

	// 4. Wait all go-routines
	time.Sleep(200 * time.Millisecond)

	// 5. Check ordered receiving
	for i := range messageCount {
		assert.Equal(t, fmt.Sprintf("msg-%d", i), results[i], "message out of order for waiter %d", i)
	}
}

func Test_Get_EmptyQueue_TimesOut(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	// 1. PUT message
	msg := valueobject.Message{Content: "test message"}
	body, _ := json.Marshal(msg)
	req := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusOK, resp.Code)

	// 2. GET message
	req2 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)
	assert.Equal(t, http.StatusOK, resp2.Code)

	// 3. GET message from empty queue with timeout
	req3 := httptest.NewRequest(http.MethodGet, "/queue/my-test?timeout=1", nil)
	resp3 := httptest.NewRecorder()

	start := time.Now()
	httpHandler.ServeHTTP(resp3, req3)
	duration := time.Since(start)

	assert.Equal(t, http.StatusNotFound, resp3.Code)
	assert.GreaterOrEqual(t, duration, 1*time.Second)
	assert.Less(t, duration, 10*time.Second)
}

func Test_Put_InvalidMessage(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	req := httptest.NewRequest(http.MethodPut, "/queue/test", bytes.NewReader([]byte(`{}`)))
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusBadRequest, resp.Code)
}

func Test_Get_MissingQueueName(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	req := httptest.NewRequest(http.MethodGet, "/queue/", nil)
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)

	assert.Equal(t, http.StatusNotFound, resp.Code)
}

func Test_Broker_IsFull(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(1, 10, 10)

	msg := valueobject.Message{Content: "message A"}
	body, _ := json.Marshal(msg)
	req := httptest.NewRequest(http.MethodPut, "/queue/queue1", bytes.NewReader(body))
	resp := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp, req)
	assert.Equal(t, http.StatusOK, resp.Code)

	// Must get rejected
	req2 := httptest.NewRequest(http.MethodPut, "/queue/queue2", bytes.NewReader(body))
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)

	assert.Equal(t, http.StatusConflict, resp2.Code)
}

func Test_Queue_IsFull(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 1, 10)

	msg := valueobject.Message{Content: "first"}
	body1, _ := json.Marshal(msg)
	req1 := httptest.NewRequest(http.MethodPut, "/queue/small", bytes.NewReader(body1))
	resp1 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp1, req1)
	assert.Equal(t, http.StatusOK, resp1.Code)

	// Must get rejected
	msg2 := valueobject.Message{Content: "second"}
	body2, _ := json.Marshal(msg2)
	req2 := httptest.NewRequest(http.MethodPut, "/queue/small", bytes.NewReader(body2))
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)

	assert.Equal(t, http.StatusConflict, resp2.Code)
}

func Test_Close_Request(t *testing.T) {
	t.Parallel()

	httpHandler := getHttpHandler(10, 10, 10)

	// 1. make empty queue
	msg1 := valueobject.Message{Content: "test message"}
	body1, _ := json.Marshal(msg1)
	req1 := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body1))
	resp1 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp1, req1)
	assert.Equal(t, http.StatusOK, resp1.Code)

	// 2. clear queue
	req2 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
	resp2 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp2, req2)
	assert.Equal(t, http.StatusOK, resp2.Code)

	// Run test

	// 3. GET message from empty queue with cancellation(create and then delete waiter)
	ctx, cancel := context.WithCancel(t.Context())
	req3 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil).WithContext(ctx)
	resp3 := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		httpHandler.ServeHTTP(resp3, req3)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("handler did not return after context cancel")
	}

	// 4. PUT message
	msg2 := valueobject.Message{Content: "test message 2"}
	body2, _ := json.Marshal(msg2)
	req4 := httptest.NewRequest(http.MethodPut, "/queue/my-test", bytes.NewReader(body2))
	resp4 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp4, req4)
	assert.Equal(t, http.StatusOK, resp4.Code)

	// 5. GET message
	req5 := httptest.NewRequest(http.MethodGet, "/queue/my-test", nil)
	resp5 := httptest.NewRecorder()
	httpHandler.ServeHTTP(resp5, req5)
	assert.Equal(t, http.StatusOK, resp5.Code)

	var msg valueobject.Message
	json.NewDecoder(resp5.Body).Decode(&msg)

	assert.Equal(t, "test message 2", msg.Content)
}
