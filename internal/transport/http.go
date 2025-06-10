package transport

import (
	"net/http"
	"strings"
)

type Action interface {
	Route() string
	Method() string
	Handle(w http.ResponseWriter, r *http.Request, params Params)
}

type Params map[string]string

type Http struct {
	actions []Action
}

func NewHttp(actions ...Action) *Http {
	return &Http{actions: actions}
}

func (h *Http) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	action, params, ok := h.findRouteHandler(r)
	if !ok {
		http.NotFound(w, r)

		return
	}

	action.Handle(w, r, params)
}

func (h *Http) findRouteHandler(r *http.Request) (Action, Params, bool) {
	if len(h.actions) == 0 {
		return nil, nil, false
	}

	pathSegments := getPathSegments(r.URL.Path)

	for _, action := range h.actions {
		if r.Method != action.Method() {
			continue
		}

		actionPathSegments := getPathSegments(action.Route())

		if params, ok := matchPathSegments(actionPathSegments, pathSegments); ok {
			return action, params, true
		}
	}

	return nil, nil, false
}

func matchPathSegments(actionPathSegments, requestPathSegments []string) (Params, bool) {
	if len(actionPathSegments) != len(requestPathSegments) {
		return nil, false
	}

	params := make(Params)

	for i, actionPathSegment := range actionPathSegments {
		if isParam(actionPathSegment) {
			key := strings.Trim(actionPathSegment, "{}")
			params[key] = requestPathSegments[i]

			continue
		}

		if actionPathSegment != requestPathSegments[i] {
			return nil, false
		}
	}

	return params, true
}

func isParam(pathSegment string) bool {
	return strings.HasPrefix(pathSegment, "{") && strings.HasSuffix(pathSegment, "}")
}

func getPathSegments(path string) []string {
	return strings.Split(strings.Trim(path, "/"), "/")
}
