package importer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"

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

func (wi *WikidataImporter) RunStage1() error {
	log.Printf("Running Stage 1")

	log.Printf(">Creating constraints")
	session := wi.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return tx.Run("CREATE CONSTRAINT ON (n:Entity) ASSERT n.id IS UNIQUE;", map[string]interface{}{})
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
	s := strings.ToUpper(propertyLabel)
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
			session := wi.driver.NewSession(neo4j.SessionConfig{})
			defer session.Close()
			//fmt.Printf("ID: %v\n", entity.ID)
			// if entity.ID == "Q2013" {
			//   for name, statements := range entity.Claims {
			//     fmt.Printf("Claim: %s\n", name)
			//     for _, statement := range statements {
			//       fmt.Printf("---------------------------\n")
			//       fmt.Printf("Statement:\n")
			//       fmt.Printf("\tID: %s\n", statement.ID)
			//       fmt.Printf("\tMainSnak:\n")
			//       printSnak(&statement.MainSnak)
			//       fmt.Printf("\tReferences:\n")
			//       fmt.Printf("\tQualifiers:\n")
			//       for qualifierName, qualifiers := range statement.Qualifiers {
			//         fmt.Printf("Qualifier name: %s\n", qualifierName)
			//         for _, qualifier := range qualifiers {
			//           printSnak(&qualifier)
			//         }
			//       }
			//       fmt.Printf("---------------------------\n")
			//     }
			//   }
			// }

			if entity.ID == "Q2013" {
				for name, statements := range entity.Claims {
					fmt.Printf("Claim: %s\n", name)
					claimId := fmt.Sprintf("%s-%s", entity.ID, name)

					// Create claim node and connect with entity and property node
					_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
						_, err := tx.Run("CREATE (c:Claim { label: $id, entityId: $entityId, propertyId: $propertyId })",
							map[string]interface{}{
								"id":         claimId,
								"entityId":   entity.ID,
								"propertyId": name,
							})

						if err != nil {
							return nil, err
						}

						return tx.Run(`
             MATCH (n:Entity { id: $entityId })
             MATCH (p:Property { id: $propertyId })
             MATCH (c:Claim { label: $claimId })

             MERGE (n)-[:HAS_CLAIM]->(c)
             MERGE (p)<-[:USES_PROPERTY]-(c)
             `, map[string]interface{}{
							"entityId":   entity.ID,
							"claimId":    claimId,
							"propertyId": name,
						})
					})

					// Read property label
					result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
						res, err := tx.Run("MATCH (p:Property { id: $propertyId }) RETURN p.label AS label", map[string]interface{}{
							"propertyId": name,
						})
						if err != nil {
							return nil, err
						}
						singleRecord, err := res.Single()
						if err != nil {
							return nil, err
						}
						return singleRecord.Values[0].(string), nil
					})

					propertyLabel := result.(string)
					relType := propertyLabelToRelationshipType(propertyLabel)
					//fmt.Printf("%s\n", relType)

					if err != nil {
						return errors.Errorf("Could not set up claim node: %v", err)
					}

					for _, statement := range statements {
						mainSnak := statement.MainSnak

						if *mainSnak.DataType == 0 {
							targetEntity := mainSnak.DataValue.Value.(mediawiki.WikiBaseEntityIDValue)

							_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
								// Connect target entity with claim node
								_, err := tx.Run(`
                 MATCH (c:Claim { label: $claimId })
                 MATCH (e:Entity { id: $entityId })
                 MERGE (e)-[:IS_TARGET_OF]->(c)
                 `, map[string]interface{}{
									"claimId":  claimId,
									"entityId": targetEntity.ID,
								})
								if err != nil {
									return nil, err
								}

								// Connect origin and target entity using relType
								return tx.Run(fmt.Sprintf(`
                 MATCH (n:Entity { id: $originId })
                 MATCH (e:Entity { id: $targetId })
                 MERGE (n)-[:%s]->(e)
                 `, relType), map[string]interface{}{
									"originId": entity.ID,
									"targetId": targetEntity.ID,
								})
							})

							if err != nil {
								return errors.Errorf("Could connect target entity with claim node: %v", err)
							}
						}

						// fmt.Printf("---------------------------\n")
						// fmt.Printf("Statement:\n")
						// fmt.Printf("\tID: %s\n", statement.ID)
						// fmt.Printf("\tMainSnak:\n")
						// printSnak(&statement.MainSnak)
						// fmt.Printf("\tReferences:\n")
						// fmt.Printf("\tQualifiers:\n")
						// for qualifierName, qualifiers := range statement.Qualifiers {
						//   fmt.Printf("Qualifier name: %s\n", qualifierName)
						//   for _, qualifier := range qualifiers {
						//     printSnak(&qualifier)
						//   }
						// }
						// fmt.Printf("---------------------------\n")
					}
				}
			}

			//       session := wi.driver.NewSession(neo4j.SessionConfig{})
			//       defer session.Close()
			//       for _, statements := range entity.Claims {
			//         for _, statement := range statements {
			//           // WikibaseItem
			//           if *statement.MainSnak.DataType == 0 {
			//             if statement.MainSnak.DataValue != nil {
			//               propertyId := statement.MainSnak.Property
			//               targetEntity := statement.MainSnak.DataValue.Value.(mediawiki.WikiBaseEntityIDValue)

			//               fmt.Printf("%s-[:%s]->%s\n", entity.ID, propertyId, targetEntity.ID)
			//             }
			//           }
			//         }
			//       }
			return nil
		},
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

	err := mediawiki.Process(context.Background(), processConfig)
	if err != nil {
		return errors.Errorf("error while processing dump: %v", err)
	}

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
