package valueobject

type Message struct {
	Content string `json:"message"`
}

func (m Message) IsValid() bool {
	return m.Content != ""
}
