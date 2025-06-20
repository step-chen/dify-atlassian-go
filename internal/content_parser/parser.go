package content_parser

import "fmt"

func NewParserByType(parserType string) (IParser, error) {
	switch parserType {
	case "xml_test_case":
		return NewTCXParser(), nil
	case "": // Default parser if not specified
		return NewParser(), nil
	default:
		return nil, fmt.Errorf("unsupported parser type: %s", parserType)
	}
}

type Parser struct {
}

func NewParser() IParser {
	return &Parser{}
}

func (p *Parser) Parse(content string) ([]ParsedContent, error) {
	var docs []ParsedContent
	docs = append(docs, ParsedContent{
		Content:  content,
		Keywords: []string{},
	})

	return docs, nil
}

func (p *Parser) ParseBytes(content []byte) ([]ParsedContent, error) {
	var docs []ParsedContent
	docs = append(docs, ParsedContent{
		Content:  string(content),
		Keywords: []string{},
	})

	return docs, nil
}
