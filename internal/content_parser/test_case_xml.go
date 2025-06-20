package content_parser

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"strings"
)

// tcxDBModelTest represents the root element of the XML.
// This struct is unexported (private) as it's an internal detail of the parsing process.
type tcxDBModelTest struct {
	XMLName     xml.Name        `xml:"dbmodeltest"`
	Validations []tcxValidation `xml:"validation"`
}

// tcxValidation represents a single 'validation' element in the XML.
// Not directly exposed, but its data is used to construct TCXValidationDoc.
type tcxValidation struct {
	XMLName      xml.Name        `xml:"validation"`
	ID           string          `xml:"id,attr"`
	Name         string          `xml:"name"`
	Status       string          `xml:"status"`
	Prerequisite tcxPrerequisite `xml:"prerequisite"`
	Purpose      tcxPurpose      `xml:"purpose"`
	ActionList   tcxActionList   `xml:"actionlist"`
	Output       tcxOutput       `xml:"output"`
	PreQuery     tcxCDATA        `xml:"pre_query"`
	Query        tcxCDATA        `xml:"query"`
	PostQuery    tcxCDATA        `xml:"post_query"`
}

// tcxPrerequisite represents the 'prerequisite' element. Not exposed.
type tcxPrerequisite struct {
	Versions tcxVersions `xml:"versions"`
	BB       string      `xml:"bb"`
}

// tcxVersions represents the 'versions' element. Not exposed.
type tcxVersions struct {
	Min string `xml:"min,attr"`
	Max string `xml:"max,attr"`
}

// tcxPurpose represents the 'purpose' element. Not exposed.
type tcxPurpose struct {
	SpecStatement string   `xml:"specStatement"`
	DocLinks      []string `xml:"doclinks"`
}

// tcxActionList represents the 'actionlist' element. Not exposed.
type tcxActionList struct {
	Actions []string `xml:"action"`
}

// etsOutput represents the 'output' element. Not exposed.
type tcxOutput struct {
	ErrorDescription string `xml:"errordescription"`
	ErrorMessage     string `xml:"errormessage"`
}

// tcxTableNameRegex is a pre-compiled regex to find table names within SQL queries.
// It's private because it's an internal utility.
var tcxTableNameRegex = regexp.MustCompile(
	`\b(?:FROM|JOIN|UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+` +
		`([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)`,
)

// tcxCDATA is a custom type to handle CDATA sections for SQL queries.
// It's private as it's an internal representation of XML data.
type tcxCDATA string

// UnmarshalXML unmarshals CDATA sections for tcxCDATA type.
// This method is part of the xml.Unmarshaler interface for tcxCDATA,
// and its visibility follows the type's visibility.
func (c *tcxCDATA) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string
	err := d.DecodeElement(&s, &start)
	*c = tcxCDATA(strings.TrimSpace(s))
	return err
}

// GetTableNames extracts unique table names from the SQL content of the tcxCDATA.
// This method is part of the private tcxCDATA type, so it's not directly callable from outside the package.
func (c tcxCDATA) GetTableNames() []string {
	if c == "" {
		return nil
	}

	extractedTables := make(map[string]struct{}) // Use a map for unique table names
	matches := tcxTableNameRegex.FindAllStringSubmatch(string(c), -1)
	for _, match := range matches {
		if len(match) > 1 {
			tableName := strings.TrimSpace(match[1])
			if tableName != "" {
				extractedTables[tableName] = struct{}{}
			}
		}
	}

	var names []string
	for table := range extractedTables {
		names = append(names, strings.Split(table, ".")...)
	}
	return names
}

// TCXDefaultParser is a concrete implementation of the XMLParser interface
// for TCX validation documents.
// It's exported so it can be instantiated from other packages if needed,
// though typically users would rely on the NewTCXParser constructor.
type TCXParser struct {
	// This struct could hold configuration or dependencies in the future,
	// for example, a custom logger or different regex patterns.
}

// NewTCXParser creates a new instance of TCXDefaultParser, returning it as
// an XMLParser interface type. This is the idiomatic way to provide
// an implementation of an interface.
func NewTCXParser() IParser {
	return &TCXParser{}
}

// Parse reads the XML content from a string, parses it,
// and returns a slice of formatted RAG document strings along with extracted keywords.
// This method implements the Parse method of the XMLParser interface.
func (p *TCXParser) Parse(content string) ([]ParsedContent, error) {
	var tcxDBModelTest tcxDBModelTest
	err := xml.Unmarshal([]byte(content), &tcxDBModelTest)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling XML: %w", err)
	}

	return p.tcxExtractAndGenerateValidationDocs(&tcxDBModelTest), nil
}

func (p *TCXParser) ParseBytes(content []byte) ([]ParsedContent, error) {
	var tcxDBModelTest tcxDBModelTest
	err := xml.Unmarshal(content, &tcxDBModelTest)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling XML: %w", err)
	}

	return p.tcxExtractAndGenerateValidationDocs(&tcxDBModelTest), nil
}

// tcxExtractKeywordsForValidation extracts all relevant keywords for a single validation.
// This function is private (unexported) because its name starts with a lowercase letter.
func (p *TCXParser) tcxExtractKeywordsForValidation(validation tcxValidation) []string {
	var keywords []string

	// 1. Add BB content as a keyword
	if validation.Prerequisite.BB != "" {
		keywords = append(keywords, validation.Prerequisite.BB)
	}

	// 2. Extract table names using the GetTableNames method
	keywords = append(keywords, validation.PreQuery.GetTableNames()...)
	keywords = append(keywords, validation.Query.GetTableNames()...)
	keywords = append(keywords, validation.PostQuery.GetTableNames()...)

	// Remove duplicate keywords
	uniqueKeywordsMap := make(map[string]struct{})
	var finalKeywords []string
	for _, k := range keywords {
		if _, exists := uniqueKeywordsMap[k]; !exists {
			uniqueKeywordsMap[k] = struct{}{}
			finalKeywords = append(finalKeywords, k)
		}
	}
	return finalKeywords
}

// tcxExtractAndGenerateValidationDocs takes a tcxDBModelTest struct and processes each validation
// to extract keywords and generate formatted content. It returns a slice of TCXValidationDoc.
// This function is private (unexported) because its name starts with a lowercase letter.
func (p *TCXParser) tcxExtractAndGenerateValidationDocs(tcxDBModelTest *tcxDBModelTest) []ParsedContent {
	var validationDocs []ParsedContent

	for _, validation := range tcxDBModelTest.Validations {
		var sb strings.Builder // Builder for the document content

		// --- Extract Keywords using the new helper function ---
		keywords := p.tcxExtractKeywordsForValidation(validation)

		// --- Generate Document Content ---
		sb.WriteString(fmt.Sprintf("## Validation: %s\n", validation.ID))
		sb.WriteString(fmt.Sprintf("**Name:** %s\n", validation.Name))
		sb.WriteString(fmt.Sprintf("**Status:** %s\n", validation.Status))
		sb.WriteString("**Prerequisites:**\n")
		sb.WriteString(fmt.Sprintf("* Versions: min=\"%s\", max=\"%s\"\n", validation.Prerequisite.Versions.Min, validation.Prerequisite.Versions.Max))
		sb.WriteString(fmt.Sprintf("* Building Block (BB): %s\n", validation.Prerequisite.BB))

		if validation.Purpose.SpecStatement != "" || len(validation.Purpose.DocLinks) > 0 {
			sb.WriteString("**Purpose:**\n")
		}
		if validation.Purpose.SpecStatement != "" {
			sb.WriteString(fmt.Sprintf("* Specification Statement: %s\n", validation.Purpose.SpecStatement))
		}
		if len(validation.Purpose.DocLinks) > 0 {
			sb.WriteString("* Documentation Links:\n")
			for _, link := range validation.Purpose.DocLinks {
				sb.WriteString(fmt.Sprintf("    * %s\n", link))
			}
		}
		if validation.Purpose.SpecStatement != "" || len(validation.Purpose.DocLinks) > 0 {
			sb.WriteString("\n")
		}

		if len(validation.ActionList.Actions) > 0 {
			sb.WriteString("**Actions:**\n")
			for _, action := range validation.ActionList.Actions {
				sb.WriteString(fmt.Sprintf("* %s\n", action))
			}
		}
		sb.WriteString("\n")

		if validation.Output.ErrorDescription != "" || validation.Output.ErrorMessage != "" {
			sb.WriteString("**Output (Error Information):**\n")
		}
		if validation.Output.ErrorDescription != "" {
			sb.WriteString(fmt.Sprintf("* **Error Description:** %s\n", validation.Output.ErrorDescription))
		}
		if validation.Output.ErrorMessage != "" {
			sb.WriteString(fmt.Sprintf("* **Error Message Format:** %s\n", validation.Output.ErrorMessage))
		}
		if validation.Output.ErrorDescription != "" || validation.Output.ErrorMessage != "" {
			sb.WriteString("\n")
		}

		if validation.PreQuery != "" || validation.Query != "" || validation.PostQuery != "" {
			sb.WriteString("**Query Details:**\n")
		}
		if validation.PreQuery != "" {
			sb.WriteString("* **Pre-Query:**\n")
			sb.WriteString("```sql\n")
			sb.WriteString(string(validation.PreQuery))
			sb.WriteString("\n```\n")
		}

		if validation.Query != "" {
			sb.WriteString("* **Main Query:**\n")
			sb.WriteString("```sql\n")
			sb.WriteString(string(validation.Query))
			sb.WriteString("\n```\n")
		}

		if validation.PostQuery != "" {
			sb.WriteString("* **Post-Query:**\n")
			sb.WriteString("```sql\n")
			sb.WriteString(string(validation.PostQuery))
			sb.WriteString("\n```\n")
		}
		if validation.PreQuery != "" || validation.Query != "" || validation.PostQuery != "" {
			sb.WriteString("\n")
		}

		// Append the combined document and keywords to the slice
		validationDocs = append(validationDocs, ParsedContent{
			Content:  sb.String(),
			Keywords: keywords,
		})
	}

	return validationDocs
}
