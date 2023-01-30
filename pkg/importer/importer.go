package importer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/mrz1836/go-sanitize"
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

type ClaimPair struct {
	startID    string
	targetID   string
	qualifiers map[string][]mediawiki.Snak
}

type WikidataImporter struct {
	Config *Neo4JConfig

	httpClient *retryablehttp.Client
	url        string
	driver     neo4j.Driver
	dumpPath   string
	mtx        sync.Mutex

	batchMap map[string][]ClaimPair
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
		batchMap:   make(map[string][]ClaimPair),
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

func (wi *WikidataImporter) RunStage1() error {
	log.Printf("Running Stage 1")

	log.Printf(">Creating constraints")
	session := wi.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("CREATE CONSTRAINT uniqueId FOR (n:Entity) REQUIRE (n.id) IS UNIQUE;", map[string]interface{}{})
	})
	if err != nil {
		return fmt.Errorf("Could not create constraints: %v", err)
	}

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
			session := wi.driver.NewSession(neo4j.SessionConfig{})
			defer session.Close()

			_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
				extraLabel := []string{"Item", "Property", "Mediainfo"}[entity.Type]
				return tx.Run(fmt.Sprintf("CREATE (n:%s:Entity { id: $id, pageId: $pageId, label: $label, description: $description })", extraLabel), map[string]interface{}{
					"id":          entity.ID,
					"pageId":      entity.PageID,
					"label":       entity.Labels["en"].Value,
					"description": entity.Descriptions["en"].Value,
				})
			})

			if err != nil {
				return errors.Errorf("failed to write entity %v: %v", entity.ID, err)
			}

			return nil
		},
	}

	err = mediawiki.Process(context.Background(), processConfig)
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

func propertyLabelToRelationshipType(propertyLabel string) string {
	s := sanitize.AlphaNumeric(propertyLabel, true)
	s = strings.ToUpper(s)
	return strings.ReplaceAll(s, " ", "_")
}

func (wi *WikidataImporter) RunStage2() error {
	log.Printf("Running Stage 2")

	log.Printf(">Linking statements")
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

			for propertyName, statements := range entity.Claims {
				var claims []ClaimPair
				for _, statement := range statements {
					mainSnak := statement.MainSnak

					if mainSnak.DataType != nil && mainSnak.DataValue != nil && *mainSnak.DataType == 0 {
						targetEntity := mainSnak.DataValue.Value.(mediawiki.WikiBaseEntityIDValue)

						claims = append(claims, ClaimPair{entity.ID, targetEntity.ID, statement.Qualifiers})
					}
				}
				if len(claims) > 0 {
					wi.mtx.Lock()
					_, ok := wi.batchMap[propertyName]
					if ok {
						wi.batchMap[propertyName] = append(wi.batchMap[propertyName], claims...)
					} else {
						wi.batchMap[propertyName] = claims
					}
					wi.mtx.Unlock()
				}
			}

			wi.mtx.Lock()
			defer wi.mtx.Unlock()

			//if len(wi.batchMap) >= 1 {
			if len(wi.batchMap) >= 500 {
				//fmt.Printf("%v\n", len(wi.batchMap))
				log.Printf("Commit!\n")
				err := wi.commitStage2Batch()
				if err != nil {
					log.Printf("%v\n", err)
					return errors.Errorf("Failed to commit batch: %v", err)
				}
				wi.batchMap = make(map[string][]ClaimPair)
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

func dataValueToString(value interface{}) string {
	idValue, ok := value.(mediawiki.WikiBaseEntityIDValue)
	if ok {
		return idValue.ID
	}

	return fmt.Sprintf("%v", value)
}

func (wi *WikidataImporter) commitStage2Batch() error {
	session := wi.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		for propertyId, pairs := range wi.batchMap {
			// Get the property name
			res, err := tx.Run("MATCH (p:Property { id: $propertyId }) RETURN p.label AS label LIMIT 1",
				map[string]interface{}{
					"propertyId": propertyId,
				})
			if err != nil {
				log.Printf("Unable to find property: %v\n", err)
				continue
			}

			singleRecord, err := res.Single()
			if err != nil {
				log.Printf("%v\n", propertyId)
				return nil, err
			}
			relType := propertyLabelToRelationshipType(singleRecord.Values[0].(string))

			var batch []map[string]interface{}
			for _, pair := range pairs {
				qualifierMap := make(map[string]interface{})
				for qualifierName, qualifiers := range pair.qualifiers {
					for _, qualifier := range qualifiers {
						if qualifier.DataValue != nil {
							qualifierFullname := qualifierName
							res, err := tx.Run("MATCH (p:Property { id: $propertyId }) RETURN p.label AS label LIMIT 1",
								map[string]interface{}{
									"propertyId": qualifierName,
								})
							singleRecord, err := res.Single()
							if err == nil {
								qualifierFullname = singleRecord.Values[0].(string)
							}

							qualifierMap[qualifierFullname] = dataValueToString(qualifier.DataValue.Value)
						}
					}
				}

				batch = append(batch, map[string]interface{}{
					"startID":    pair.startID,
					"endID":      pair.targetID,
					"qualifiers": qualifierMap,
				})
			}

			query := `
      UNWIND $batch AS row
      MERGE (start:Entity {id: row.startID})
      MERGE (end:Entity {id: row.endID})
      MERGE (start)-[r:` + fmt.Sprintf("`%s`", relType) + `]->(end)
      SET r = row.qualifiers
      `

			_, err = tx.Run(query, map[string]interface{}{
				"batch": batch,
			})
			if err != nil {
				log.Printf("Error executing batch: %v", err)
			}
		}

		return nil, nil
	})

	return err
}

func (wi *WikidataImporter) RunStage3() error {
	log.Printf("Running Stage 3")

	return nil
}

func (wi *WikidataImporter) Close() {
	wi.driver.Close()
}
