package query

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// TemplateConfig is the structure for template expansion config files.
type TemplateConfig struct {
	Templates []Template `yaml:"templates"`
}

// Template defines a query pattern with variables for cartesian expansion.
type Template struct {
	Name      string              `yaml:"name"`
	Pattern   string              `yaml:"pattern"`
	Variables map[string][]string `yaml:"variables"`
}

// LoadTemplates reads template definitions from a YAML file.
func LoadTemplates(path string) (*TemplateConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("generator: reading %s: %w", path, err)
	}
	var cfg TemplateConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("generator: parsing %s: %w", path, err)
	}
	return &cfg, nil
}

// Generate produces all queries from template expansion (cartesian product).
// Returns deduplicated queries with their template ID.
func Generate(templates []Template) map[string][]string {
	result := make(map[string][]string)
	for _, t := range templates {
		queries := expandTemplate(t)
		result[t.Name] = queries
	}
	return result
}

// expandTemplate produces the cartesian product of all variable values
// substituted into the pattern.
func expandTemplate(t Template) []string {
	// Collect variable names in pattern order.
	var varNames []string
	for name := range t.Variables {
		if strings.Contains(t.Pattern, "{"+name+"}") {
			varNames = append(varNames, name)
		}
	}

	if len(varNames) == 0 {
		return []string{t.Pattern}
	}

	// Build cartesian product.
	var results []string
	seen := make(map[string]bool)
	var expand func(pattern string, varIdx int)
	expand = func(pattern string, varIdx int) {
		if varIdx >= len(varNames) {
			q := strings.TrimSpace(pattern)
			key := strings.ToLower(q)
			if !seen[key] {
				seen[key] = true
				results = append(results, q)
			}
			return
		}
		varName := varNames[varIdx]
		placeholder := "{" + varName + "}"
		for _, val := range t.Variables[varName] {
			newPattern := strings.ReplaceAll(pattern, placeholder, val)
			expand(newPattern, varIdx+1)
		}
	}
	expand(t.Pattern, 0)
	return results
}
