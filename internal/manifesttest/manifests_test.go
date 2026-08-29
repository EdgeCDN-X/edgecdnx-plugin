package manifesttest

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestRepositoryYAMLManifestsParse(t *testing.T) {
	repoRoot := filepath.Join("..", "..")
	nonKubernetesYAML := map[string]struct{}{
		filepath.Clean(filepath.Join(repoRoot, "deploy", "mediamtx", "mediamtx.yml")):  {},
		filepath.Clean(filepath.Join(repoRoot, "deploy", "monitoring", "values.yaml")): {},
	}
	roots := []string{
		filepath.Join(repoRoot, "config"),
		filepath.Join(repoRoot, "deploy"),
		filepath.Join(repoRoot, "experiments", "k6"),
	}

	manifestCount := 0
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || (filepath.Ext(path) != ".yaml" && filepath.Ext(path) != ".yml") {
				return nil
			}
			if _, isApplicationConfig := nonKubernetesYAML[filepath.Clean(path)]; isApplicationConfig {
				return nil
			}
			manifestCount += parseManifestFile(t, path)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	if manifestCount == 0 {
		t.Fatal("no Kubernetes YAML documents were parsed")
	}
}

func parseManifestFile(t *testing.T, path string) int {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()

	decoder := utilyaml.NewYAMLOrJSONDecoder(file, 4096)
	documents := 0
	for index := 1; ; index++ {
		var document map[string]any
		err = decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("parse %s document %d: %v", path, index, err)
		}
		if len(document) == 0 {
			continue
		}
		documents++
		for _, field := range []string{"apiVersion", "kind"} {
			value, ok := document[field].(string)
			if !ok || strings.TrimSpace(value) == "" {
				t.Errorf("%s document %d has no non-empty %s", path, index, field)
			}
		}
	}
	return documents
}
