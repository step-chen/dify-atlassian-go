package content_parser

type ParsedContent struct {
	Content  string
	Keywords []string
}

type IParser interface {
	Parse(content string) ([]ParsedContent, error)
	ParseBytes(content []byte) ([]ParsedContent, error)
}
