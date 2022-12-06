package importer

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/mediawiki"
	"gitlab.com/tozd/go/x"
)

type Neo4JConfig struct {
  Address string
  Username string
  Password string
}

type WikidataImporter struct {
  Config *Neo4JConfig

  httpClient *retryablehttp.Client
  url string
  driver neo4j.Driver
}

func NewWikidataImporter(config *Neo4JConfig) (*WikidataImporter, error) {
  var err error

  httpClient := retryablehttp.NewClient()
  httpClient.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
    req.Header.Set("User-Agent", "Wikidata Importer")
  }

  url, err := mediawiki.LatestWikidataEntitiesRun(context.Background(), httpClient)
  if err != nil {
    return nil, err
  }

  driver, err := neo4j.NewDriver(config.Address, neo4j.BasicAuth(config.Username, config.Password, ""))
  if err != nil {
    return nil, fmt.Errorf("Unable to create Neo4j driver: %w", err)
  }

  return &WikidataImporter{
    Config: config,
    httpClient: httpClient,
    driver: driver,
    url: url,
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

  config := mediawiki.ProcessDumpConfig{
    URL: wi.url,
    Path: "dump",
    Client: wi.httpClient,
    Progress: func(c context.Context, prog x.Progress) {
      fmt.Printf("Progress: %v\nEstimated: %v\n", prog.Percent(), prog.Estimated())
    },
  }
  
  log.Printf(">Creating entities")
  err := mediawiki.ProcessWikidataDump(context.Background(), &config, func(c context.Context, entity mediawiki.Entity) errors.E {
    // fmt.Printf("%v\n", entity.ID)

    session := wi.driver.NewSession(neo4j.SessionConfig{})
    defer session.Close()

    _, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
      extraLabel := []string{"Item", "Property", "Mediainfo"}[entity.Type]
      return tx.Run(fmt.Sprintf("CREATE (n:%s:Entity { id: $id })", extraLabel), map[string]interface{}{
        "id": entity.ID,
      })
    })

    if err != nil {
      return errors.Errorf("failed to write entity %v: %v", entity.ID, err)
    }

    return nil
  })
  if err != nil {
    return errors.Errorf("error while processing dump: %v", err)
  }

  return nil
}

func (wi *WikidataImporter) RunStage2() error {
  log.Printf("Running Stage 2")
  // TODO
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
