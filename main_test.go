package main

import (
	"encoding/csv"
	"regexp"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestParsing(t *testing.T) {
	// Sample lines mimicking what csv reader extracts
	// The CSV reader returns the field content *after* unescaping double quotes.
	// So `id=""...""` in the raw file becomes `id="..."` in the string.

	// Sample line 6 from file:
	// ... id=""{\""$oid\"":\""693885e2f227ce8067db8d33\""}"" ...
	// The CSV reader will parse this. Let's see what CSV reader produces first.

	rawLine := `2025-10-15T17:32:48.521Z,dsync,col2,"Dec  9 12:26:13.446 ERR Isolated retry still failed retryErr=""bulk write exception"" err=""..."" index=0 id=""{\""$oid\"":\""693885e2f227ce8067db8d33\""}"" key=1765311970851576000"`

	r := csv.NewReader(strings.NewReader(rawLine))
	record, err := r.Read()
	if err != nil {
		t.Fatalf("Failed to parse CSV line: %v", err)
	}

	message := record[3]
	t.Logf("Parsed Message: %s", message)

	// Now test regex on this message
	nsRegex := regexp.MustCompile(`collection:\s*([a-zA-Z0-9_.]+)`)
	// Adding the collection part to the rawLine for the test
	// content of message in loop:
	// Dec  9 12:26:13.446 ERR Isolated retry still failed retryErr="bulk write exception" err="..." index=0 id="{"$oid":"693885e2f227ce8067db8d33"}" key=1765311970851576000

	// Wait, the raw line I constructed above doesn't have "collection: ..." which I saw in line 6 of the file.
	// Let me check line 6 of the file again from context.
	// Line 6: ... retryErr=""bulk write exception: write errors: [E11000 duplicate key error collection: testshard.col2 index: ...

	// Re-constructing the test string to match Line 6 more closely
	csvLine := `2025-10-15,pod,proc,"Error ... collection: testshard.col2 ... id=""{\""$oid\"":\""693885e2f227ce8067db8d33\""}"" ..."`

	r = csv.NewReader(strings.NewReader(csvLine))
	record, err = r.Read()
	if err != nil {
		t.Fatalf("CSV read error: %v", err)
	}
	msg := record[3]

	// Test Namespace extraction
	nsMatch := nsRegex.FindStringSubmatch(msg)
	if len(nsMatch) < 2 {
		t.Errorf("Failed to match namespace. Msg: %s", msg)
	} else {
		if nsMatch[1] != "testshard.col2" {
			t.Errorf("Expected testshard.col2, got %s", nsMatch[1])
		}
	}

	// Test ID extraction
	idRegex := regexp.MustCompile(`id="(\{.*?\})"`)
	idMatch := idRegex.FindStringSubmatch(msg)
	if len(idMatch) < 2 {
		t.Errorf("Failed to match ID. Msg: %s", msg)
	} else {
		idJSON := idMatch[1]
		t.Logf("Extracted ID JSON: %s", idJSON)
		// Expected: {\"$oid\":\"693885e2f227ce8067db8d33\"}

		// Fix: Remove backslashes
		idJSONClean := strings.ReplaceAll(idJSON, `\"`, `"`)

		var raw bson.D
		err := bson.UnmarshalExtJSON([]byte(idJSONClean), true, &raw)
		if err != nil {
			t.Errorf("Failed to unmarshal extracted JSON: %v", err)
		}
		t.Logf("Successfully parsed ID: %v", raw)
	}
}
