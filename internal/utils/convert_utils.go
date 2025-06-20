package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/JohannesKaufmann/html-to-markdown/v2/converter"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/base"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/commonmark"
	"github.com/JohannesKaufmann/html-to-markdown/v2/plugin/table"
)

type ConversionMethod int

const (
	ConversionMethodCommon     ConversionMethod = 0 // iota
	ConversionMethodNative     ConversionMethod = 1
	ConversionMethodMarkitdown ConversionMethod = 2
	ConversionMethodPandoc     ConversionMethod = 3
)

func ConvertFile2Markdown(inputPath, mediaType, separator string, method ConversionMethod) (string, error) {
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		return "", fmt.Errorf("input file does not exist: %s", inputPath)
	}

	var (
		markdown string
		err      error
		attempts []string
	)

	switch method {
	case ConversionMethodNative:
		if mediaType != "text/html" {
			return "", fmt.Errorf("native conversion only supports text/html, got: %s", mediaType)
		}
		markdown, err = html2Markdown(inputPath, separator)
		attempts = append(attempts, "native")

	case ConversionMethodMarkitdown:
		if !dockerUtils.markitdownImage {
			return "", errors.New("markitdown docker image not available")
		}
		markdown, err = convert2MarkdownByMarkitdown(inputPath, separator)
		attempts = append(attempts, "markitdown")

	case ConversionMethodPandoc:
		if !dockerUtils.pandocImage {
			return "", errors.New("pandoc docker image not available")
		}
		markdown, err = convert2MarkdownByPandoc(inputPath, separator)
		attempts = append(attempts, "pandoc")

	case ConversionMethodCommon:
		if mediaType == "text/html" {
			if markdown, err = html2Markdown(inputPath, separator); err == nil {
				return markdown, nil
			}
			attempts = append(attempts, "native")
		}

		if dockerUtils.markitdownImage {
			if markdown, err = convert2MarkdownByMarkitdown(inputPath, separator); err == nil {
				return markdown, nil
			}
			attempts = append(attempts, "markitdown")
		}

		if dockerUtils.pandocImage {
			if markdown, err = convert2MarkdownByPandoc(inputPath, separator); err == nil {
				return markdown, nil
			}
			attempts = append(attempts, "pandoc")
		}

	default:
		return "", fmt.Errorf("invalid conversion method: %d", method)
	}

	if err == nil {
		return markdown, nil
	}

	// Track failed conversion attempts
	failedTypesMu.Lock()
	failedTypes[mediaType] = true
	failedTypesMu.Unlock()

	if len(attempts) > 0 {
		log.Printf("conversion failed for %s (attempted methods: %v): %v", inputPath, attempts, err)
		return "", fmt.Errorf("conversion failed for %s (attempted methods: %v): %w", inputPath, attempts, err)
	}

	return "", fmt.Errorf("no conversion methods available for %s", inputPath)
}

func html2Markdown(inputPath, separator string) (string, error) {
	if htmlContent, err := os.ReadFile(inputPath); err != nil {
		return "", fmt.Errorf("failed to read HTML file %s: %w", inputPath, err)
	} else {
		return html2MarkdownFromContent(string(htmlContent), separator)
	}
}

func html2MarkdownFromContent(content, separator string) (string, error) {
	conv := converter.NewConverter(
		converter.WithPlugins(
			base.NewBasePlugin(),
			commonmark.NewCommonmarkPlugin(),
			table.NewTablePlugin(),
		),
	)
	if markdown, err := conv.ConvertString(content); err != nil {
		return "", fmt.Errorf("failed to convert HTML to Markdown: %w", err)
	} else {
		return formatContent(markdown, separator), nil
	}
}

// convert2MarkdownByMarkitdown converts a file to markdown using markitdown docker image
func convert2MarkdownByMarkitdown(inputPath string, separator string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	inputFile, err := os.Open(inputPath)
	if err != nil {
		return "", fmt.Errorf("failed to open input file %s: %w", inputPath, err)
	}
	defer inputFile.Close()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", MarkitdownImage)
	cmd.Env = append(os.Environ(),
		"LANG=en_US.UTF-8",
		"LC_ALL=en_US.UTF-8",
		"PYTHONIOENCODING=utf-8",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdin = inputFile
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("markitdown conversion timed out for %s", inputPath)
		}

		if dockerUtils.pandocImage {
			fp, err := convert2DocxByPandoc(inputPath)
			defer RemoveFile(fp)
			if err != nil {
				return "", fmt.Errorf("markitdown conversion failed (original error: %v, pandoc fallback failed: %v)", stderr.String(), err)
			}
			return convert2MarkdownByMarkitdown(fp, separator)
		}

		return "", fmt.Errorf("markitdown conversion failed for %s: %v (stderr: %s)", inputPath, err, stderr.String())
	}

	return formatContent(stdout.String(), separator), nil
}

func convert2MarkdownByPandoc(inputPath string, separator string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "pandoc", inputPath, "-t", "markdown")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("pandoc conversion timed out for %s", inputPath)
		}
		return "", fmt.Errorf("pandoc conversion failed for %s: %w (stderr: %s)", inputPath, err, stderr.String())
	}

	return formatContent(stdout.String(), separator), nil
}

func convert2DocxByPandoc(inputPath string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	outputPath, err := ChangeFileExtension(inputPath, ".docx")
	if err != nil {
		return "", fmt.Errorf("failed to generate output path: %w", err)
	}

	cmd := exec.CommandContext(ctx, "pandoc", inputPath, "-o", outputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("pandoc docx conversion failed for %s: %v\nOutput: %s", inputPath, err, string(output))
	}

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		return "", fmt.Errorf("pandoc docx conversion failed - output file not created: %s", outputPath)
	}

	return outputPath, nil
}

func ConvertContent2Markdown(content, mediaType, separator string, method ConversionMethod) (string, error) {
	var (
		markdown string
		err      error
		attempts []string
	)

	switch method {
	case ConversionMethodNative:
		if mediaType != "text/html" {
			return "", fmt.Errorf("native conversion only supports text/html, got: %s", mediaType)
		}
		markdown, err = html2MarkdownFromContent(content, separator)
		attempts = append(attempts, "native")

	case ConversionMethodMarkitdown:
		if !dockerUtils.markitdownImage {
			return "", errors.New("markitdown docker image not available")
		}
		markdown, err = convert2MarkdownByMarkitdownFromContent(content, separator)
		attempts = append(attempts, "markitdown")

	case ConversionMethodCommon:
		if mediaType == "text/html" {
			if markdown, err = html2MarkdownFromContent(content, separator); err == nil {
				return markdown, nil
			}
			attempts = append(attempts, "native")
		}

		if dockerUtils.markitdownImage {
			if markdown, err = convert2MarkdownByMarkitdownFromContent(content, separator); err == nil {
				return markdown, nil
			}
			attempts = append(attempts, "markitdown")
		}

	default:
		return "", fmt.Errorf("invalid conversion method: %d", method)
	}

	if err == nil {
		return markdown, nil
	}

	// Track failed conversion attempts
	failedTypesMu.Lock()
	failedTypes[mediaType] = true
	failedTypesMu.Unlock()

	if len(attempts) > 0 {
		log.Printf("conversion failed (attempted methods: %v): %v", attempts, err)
		return "", fmt.Errorf("conversion failed (attempted methods: %v): %w", attempts, err)
	}

	return "", fmt.Errorf("no conversion methods available")
}

func convert2MarkdownByMarkitdownFromContent(content string, separator string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm", "-i", MarkitdownImage)
	cmd.Env = append(os.Environ(),
		"LANG=en_US.UTF-8",
		"LC_ALL=en_US.UTF-8",
		"PYTHONIOENCODING=utf-8",
	)

	var stdout, stderr bytes.Buffer
	cmd.Stdin = strings.NewReader(content)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "", fmt.Errorf("markitdown conversion timed out")
		}

		return "", fmt.Errorf("markitdown conversion failed: %v (stderr: %s)", err, stderr.String())
	}

	return formatContent(stdout.String(), separator), nil
}
