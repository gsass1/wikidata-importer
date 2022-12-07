package importer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/mediawiki"
	"gitlab.com/tozd/go/x"
)

type Neo4JConfig struct {
	Address  string
	Username string
	Password string
}

type WikidataImporter struct {
	Config *Neo4JConfig

	httpClient *retryablehttp.Client
	url        string
	driver     neo4j.Driver
	dumpPath   string
}

func NewWikidataImporter(config *Neo4JConfig, dumpPath string) (*WikidataImporter, error) {
	var err error

	httpClient := retryablehttp.NewClient()
	httpClient.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
		req.Header.Set("User-Agent", "Wikidata Importer")
	}

	url, err := mediawiki.LatestWikidataEntitiesRun(context.Background(), httpClient)
	if err != nil {
		return nil, err
	}

	// LatestWikidataEntitiesRun always returns the bz2 file so we have to do this..
	url = strings.Trim(url, ".bz2") + ".gz"

	driver, err := neo4j.NewDriver(config.Address, neo4j.BasicAuth(config.Username, config.Password, ""))
	if err != nil {
		return nil, fmt.Errorf("Unable to create Neo4j driver: %w", err)
	}

	return &WikidataImporter{
		Config:     config,
		httpClient: httpClient,
		driver:     driver,
		url:        url,
		dumpPath:   dumpPath,
	}, nil
}

func (wi *WikidataImporter) RunStage0() error {
	log.Printf("Running Stage 0")

	session := wi.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	log.Printf(">Cleaning database")

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("MATCH (n) DETACH DELETE n;", map[string]interface{}{})
	})
	if err != nil {
		return fmt.Errorf("Could not clean database: %w", err)
	}

	log.Printf(">Done!")

	return nil
}

const BatchSize = 10000

func (wi *WikidataImporter) RunStage1() error {
	log.Printf("Running Stage 1")

	itemBatch := make([]map[string]interface{}, BatchSize)
	itemBatchIdx := 0

	propertyBatch := make([]map[string]interface{}, BatchSize)
	propertyBatchIdx := 0

	var lock sync.Mutex

	log.Printf(">Creating entities")
	processConfig := &mediawiki.ProcessConfig[mediawiki.Entity]{
		URL:         wi.url,
		Path:        wi.dumpPath,
		Client:      wi.httpClient,
		FileType:    mediawiki.JSONArray,
		Compression: mediawiki.GZIP,
		Progress: func(c context.Context, prog x.Progress) {
			fmt.Printf("Progress: %v\nEstimated: %v\n", prog.Percent(), prog.Estimated())
		},
		Process: func(c context.Context, entity mediawiki.Entity) errors.E {
			lock.Lock()
			// TODO: doesn't include Mediainfo entity type
			if entity.Type == 0 {
				itemBatch[itemBatchIdx] = map[string]interface{}{
					"id":          entity.ID,
					"pageId":      entity.PageID,
					"label":       entity.Labels["en"].Value,
					"description": entity.Descriptions["en"].Value,
				}
				itemBatchIdx = itemBatchIdx + 1
			} else if entity.Type == 1 {
				propertyBatch[propertyBatchIdx] = map[string]interface{}{
					"id":          entity.ID,
					"pageId":      entity.PageID,
					"label":       entity.Labels["en"].Value,
					"description": entity.Descriptions["en"].Value,
				}
				propertyBatchIdx = propertyBatchIdx + 1
			}
			lock.Unlock()

			if itemBatchIdx >= BatchSize || propertyBatchIdx >= BatchSize {
				lock.Lock()
				defer lock.Unlock()
				session := wi.driver.NewSession(neo4j.SessionConfig{})
				defer session.Close()

				_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
					//extraLabel := []string{"Item", "Property", "Mediainfo"}[batch[i].Type]

					// Write items
					_, err := tx.Run(`
          UNWIND $batch as props
          CREATE (n:Item:Entity) SET n = props`,
						map[string]interface{}{"batch": itemBatch[:itemBatchIdx]})

					if err != nil {
						return nil, err
					}

					// Write properties
					_, err = tx.Run(`
          UNWIND $batch as props
          CREATE (n:Property:Entity) SET n = props`,
						map[string]interface{}{"batch": propertyBatch[:propertyBatchIdx]})

					if err != nil {
						return nil, err
					}

					fmt.Printf("Wrote batch: (%d items, %d properties)\n", itemBatchIdx, propertyBatchIdx)

					itemBatchIdx = 0
					propertyBatchIdx = 0

					return nil, nil
				})

				if err != nil {
					return errors.Errorf("failed to write batch: %v", err)
				}
			}

			return nil
		},
	}

	err := mediawiki.Process(context.Background(), processConfig)
	if err != nil {
		return errors.Errorf("error while processing dump: %v", err)
	}

	return nil
}

func printSnak(snak *mediawiki.Snak) {
	fmt.Printf("Property: %s\n", snak.Property)
	fmt.Printf("Type: %v\n", snak.SnakType)
	fmt.Printf("Data Type: %v\n", *snak.DataType)
	if snak.DataValue != nil {
		fmt.Printf("Data Value: %v\n", snak.DataValue.Value)
	} else {
		fmt.Printf("Data Value: nil\n")
	}
}

func (wi *WikidataImporter) RunStage2() error {
	log.Printf("Running Stage 2")

	config := mediawiki.ProcessDumpConfig{
		URL:    wi.url,
		Path:   wi.dumpPath,
		Client: wi.httpClient,
		Progress: func(c context.Context, prog x.Progress) {
			fmt.Printf("Progress: %v\nEstimated: %v\n", prog.Percent(), prog.Estimated())
		},
	}

	log.Printf(">Linking statements")
	err := mediawiki.ProcessWikidataDump(context.Background(), &config, func(c context.Context, entity mediawiki.Entity) errors.E {
		//fmt.Printf("ID: %v\n", entity.ID)
		if entity.ID == "Q103" {
			for name, statements := range entity.Claims {
				fmt.Printf("Claim: %s\n", name)
				for _, statement := range statements {
					fmt.Printf("---------------------------\n")
					fmt.Printf("Statement:\n")
					fmt.Printf("\tID: %s\n", statement.ID)
					fmt.Printf("\tMainSnak:\n")
					printSnak(&statement.MainSnak)
					fmt.Printf("\tQualifiers:\n")
					for qualifierName, qualifiers := range statement.Qualifiers {
						fmt.Printf("Qualifier name: %s\n", qualifierName)
						for _, qualifier := range qualifiers {
							printSnak(&qualifier)
						}
					}
					fmt.Printf("---------------------------\n")
				}
			}
		}

		// session := wi.driver.NewSession(neo4j.SessionConfig{})
		// defer session.Close()

		// _, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		//   for name, statements := range entity.Claims {
		//     for _, statement := range statements {
		//       query := fmt.Sprintf(`
		//       MATCH (start:Entity), (end:Entity)
		//       WHERE
		//       start.id = $startId AND
		//       end.id = $endId
		//       WITH start, end
		//       MERGE (start)-[:%s {by: $prop, id: $claimId}]->(end)
		//       `, name)
		//       return tx.Run(query, map[string]interface{}{
		//         "startId": statement.References[0].Sn,
		//       })
		//     }
		//   }

		//   return nil, nil
		// })

		// if err != nil {
		//   return errors.Errorf("failed to write entity %v: %v", entity.ID, err)
		// }

		return nil
	})
	if err != nil {
		return errors.Errorf("error while processing dump: %v", err)
	}

	// return nil
	return nil
}

func (wi *WikidataImporter) RunStage3() error {
	log.Printf("Running Stage 3")
	// TODO
	return nil
}

func (wi *WikidataImporter) Close() {
	wi.driver.Close()
}
