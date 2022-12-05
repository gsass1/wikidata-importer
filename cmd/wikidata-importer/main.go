package main

import (
	"context"
	"fmt"
	"net/http"
	"log"

	"github.com/hashicorp/go-retryablehttp"
	"gitlab.com/tozd/go/errors"
	"gitlab.com/tozd/go/mediawiki"
)

func main() {
  client := retryablehttp.NewClient()
  client.RequestLogHook = func(logger retryablehttp.Logger, req *http.Request, retry int) {
    req.Header.Set("User-Agent", "My bot (user@example.com)")
  }

  ctx := context.Background()

  lastURL, err := mediawiki.LatestWikidataEntitiesRun(ctx, client)
  if err != nil {
    log.Fatal(err)
  }

  config := mediawiki.ProcessDumpConfig{
    URL: lastURL,
    Path: "./dump",
    Client: client,
  }

  mediawiki.ProcessWikidataDump(ctx, &config, func(c context.Context, entity mediawiki.Entity) errors.E {
    fmt.Printf("ID: %v\n", entity.ID)
    // if entity.ID == "Q103" {
    //   fmt.Printf("Claims: %v\n", entity.Claims["P136"][0].References[0])
    // }
    return nil
  })
}
