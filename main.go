package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Config holds the application configuration
type Config struct {
	LogFile string
	Source  string
	Dest    string
}

// LogEntry represents a row in the CSV
type LogEntry struct {
	Date       string
	PodName    string
	ProcessKey string
	Message    string
}

// CheckResult holds the result of a comparison
type CheckResult struct {
	Namespace string
	ID        interface{}
	Status    string // "Match", "Mismatch", "MissingInSource", "MissingInDest", "Error"
	Details   string
}

// Stats holds statistics per namespace
type Stats struct {
	TotalChecks     int
	Matches         int
	Mismatches      int
	MissingInSource int
	MissingInDest   int
	Errors          int
}

func main() {
	// Parse flags
	configFile := flag.String("logfile", "", "Path to the CSV log file")
	sourceURI := flag.String("source", "", "Source MongoDB connection string")
	destURI := flag.String("dest", "", "Destination MongoDB connection string")
	flag.Parse()

	if *configFile == "" || *sourceURI == "" || *destURI == "" {
		fmt.Println("Usage: error_checker -logfile <path> -source <uri> -dest <uri>")
		os.Exit(1)
	}

	// Connect to MongoDBs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	srcClient, err := connectMongo(ctx, *sourceURI)
	if err != nil {
		log.Fatalf("Failed to connect to source: %v", err)
	}
	defer srcClient.Disconnect(context.Background())

	destClient, err := connectMongo(ctx, *destURI)
	if err != nil {
		log.Fatalf("Failed to connect to destination: %v", err)
	}
	defer destClient.Disconnect(context.Background())

	// Open CSV
	f, err := os.Open(*configFile)
	if err != nil {
		log.Fatalf("Cannot open log file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	// Read header
	if _, err := reader.Read(); err != nil {
		log.Fatalf("Failed to read header: %v", err)
	}

	// Regex for extraction
	// Pattern for sample: collection: testshard.col2 ... id="{"$oid":"693885e2f227ce8067db8d33"}"
	// We need to be careful about the quoting in the CSV message field.
	// The CSV reader handles the outer quotes. inside message:
	// val="... collection: <ns> ... id=""<json>"" ..."
	// Note: The sample showing `id=“{\""$oid...` suggests some smart quotes or mixed quoting might be in play,
	// but the provided "raw" view showed standard quotes escaped by CSV rules.
	// Let's assume standard ASCII double quotes for property values.

	nsRegex := regexp.MustCompile(`collection:\s*([a-zA-Z0-9_.]+)`)
	// Captures the JSON content inside id=""..."" or id="..."
	// The sample shows id=""{...}"" which implies inside the CSV string it was id="{...}".
	// Wait, the CSV parser will give us the raw string of the Message column.
	// In that raw string, it likely looks like: ... id="{...}" ...
	// The sample line 6 says: ... id=""{\""$oid\"":\""693885e2f227ce8067db8d33\""}"" ...
	// When Go's CSV reader parses this, it will resolve the double double-quotes.
	// So the string in memory will be: ... id="{"$oid":"69..."}" ...
	idRegex := regexp.MustCompile(`id="(\{.*?\})"`)

	statsMap := make(map[string]*Stats)
	var discrepancyList []CheckResult

	// We shouldn't execute queries sequentially if the file is huge, but for simplicity and safety against rate limits,
	// let's do sequential or a small worker pool. Sequential is safer for now unless requested otherwise.

	lineNum := 1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV line %d: %v", lineNum, err)
			continue
		}
		lineNum++

		message := record[3]

		if !strings.Contains(message, "Isolated retry still failed") {
			continue
		}

		// Extract Namespace
		nsMatch := nsRegex.FindStringSubmatch(message)
		if len(nsMatch) < 2 {
			// Could not find namespace
			continue
		}
		namespace := nsMatch[1]

		// Extract ID
		idMatch := idRegex.FindStringSubmatch(message)
		fmt.Printf("idMatch: %v\n", idMatch)

		var idVal interface{}
		if len(idMatch) >= 2 {
			idJSON := idMatch[1]
			// Need to parse Extended JSON
			// UnmarshalExtJSON is available in mongo-driver/bson
			// But it expects keys to be quoted. The string extracted should be standard JSON.

			// The sample has `{\""$oid\"":\""...\""}` inside the CSV value.
			// CSV Reader cleans up the `""` -> `"`.
			// However, it seems the file has literal backslashes escaping the quotes as well: `\"`.
			// So we get `{\" $oid...`. We need to strip those backslashes.
			idJSONClean := strings.ReplaceAll(idJSON, `\"`, `"`)

			var id primitive.ObjectID
			err := id.UnmarshalJSON([]byte(idJSONClean))
			if err != nil {
				log.Printf("Line %d: Failed to parse ID JSON '%s' (cleaned: '%s'): %v", lineNum, idJSON, idJSONClean, err)
				continue
			}
			// For finding, we can usually use the raw BSON or specific _id field
			// If it's just an OID, `raw` usually contains `_id`? No, the string is just the value of `_id`.
			// So `raw` IS the value of `_id`.
			idVal = id
		}

		if idVal == nil {
			continue
		}

		// Perform Check
		// Split namespace
		parts := strings.SplitN(namespace, ".", 2)
		if len(parts) != 2 {
			log.Printf("Line %d: Invalid namespace %s", lineNum, namespace)
			continue
		}
		dbName, colName := parts[0], parts[1]

		res := checkDoc(context.TODO(), srcClient, destClient, dbName, colName, idVal)
		res.Namespace = namespace

		// Update stats
		if _, ok := statsMap[namespace]; !ok {
			statsMap[namespace] = &Stats{}
		}
		s := statsMap[namespace]
		s.TotalChecks++

		switch res.Status {
		case "Match":
			s.Matches++
		case "Mismatch":
			s.Mismatches++
			discrepancyList = append(discrepancyList, res)
		case "MissingInSource":
			s.MissingInSource++
			discrepancyList = append(discrepancyList, res)
		case "MissingInDest":
			s.MissingInDest++
			discrepancyList = append(discrepancyList, res)
		case "Error":
			s.Errors++
			log.Printf("Line %d: Error checking doc: %v", lineNum, res.Details)
		}
	}

	// Print Report
	fmt.Println("\n=== Analysis Report ===")
	for ns, s := range statsMap {
		fmt.Printf("\nNamespace: %s\n", ns)
		fmt.Printf("  Total Checks: %d\n", s.TotalChecks)
		fmt.Printf("  Matches: %d\n", s.Matches)
		fmt.Printf("  Mismatches: %d\n", s.Mismatches)
		fmt.Printf("  Missing in Source: %d\n", s.MissingInSource)
		fmt.Printf("  Missing in Dest: %d\n", s.MissingInDest)
		fmt.Printf("  Errors: %d\n", s.Errors)
	}

	if len(discrepancyList) > 0 {
		fmt.Println("\n=== Discrepancies ===")
		for _, d := range discrepancyList {
			fmt.Printf("[%s] ID: %v | Status: %s | Details: %s\n", d.Namespace, d.ID, d.Status, d.Details)
		}
	}
}

func connectMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	// Ping to verify
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func checkDoc(ctx context.Context, src, dest *mongo.Client, db, col string, id interface{}) CheckResult {
	var srcDoc, destDoc bson.Raw
	var srcMissing, destMissing bool

	// Find in Source
	err := src.Database(db).Collection(col).FindOne(ctx, bson.M{"_id": id}).Decode(&srcDoc)
	if err == mongo.ErrNoDocuments {
		srcMissing = true
	} else if err != nil {
		return CheckResult{ID: id, Status: "Error", Details: fmt.Sprintf("Source error: %v", err)}
	}

	// Find in Dest
	err = dest.Database(db).Collection(col).FindOne(ctx, bson.M{"_id": id}).Decode(&destDoc)
	if err == mongo.ErrNoDocuments {
		destMissing = true
	} else if err != nil {
		return CheckResult{ID: id, Status: "Error", Details: fmt.Sprintf("Dest error: %v", err)}
	}

	// If both are missing, that's a match (both sides agree the doc doesn't exist)
	if srcMissing && destMissing {
		return CheckResult{ID: id, Status: "Match", Details: "Document missing from both databases"}
	}

	// If only one is missing, that's a discrepancy
	if srcMissing {
		return CheckResult{ID: id, Status: "MissingInSource"}
	}
	if destMissing {
		return CheckResult{ID: id, Status: "MissingInDest"}
	}

	// Compare documents (both exist)
	// bson.Raw represents the raw bytes. We can compare bytes directly if key order is guaranteed same,
	// but MongoDB doesn't guarantee key order is preserved across replications/moves exactly the same way always?
	// Actually, usually it does, but canonical comparison is safer.
	// However, simplest check is bytes equal. If not, unmarshal to maps and DeepEqual.

	if string(srcDoc) == string(destDoc) {
		return CheckResult{ID: id, Status: "Match"}
	}

	// Deep comparison
	var srcMap, destMap map[string]interface{}
	_ = bson.Unmarshal(srcDoc, &srcMap)   // Ignorning error as we just decoded it
	_ = bson.Unmarshal(destDoc, &destMap) // Ignorning error as we just decoded it

	if fmt.Sprintf("%v", srcMap) == fmt.Sprintf("%v", destMap) {
		return CheckResult{ID: id, Status: "Match"}
	}

	return CheckResult{ID: id, Status: "Mismatch"}
}
