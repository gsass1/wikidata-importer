package main

import (
	"flag"
	"log"

	"git.thm.de/gsas42/wikidata-importer/pkg/importer"
)

func main() {
	neo4jAddress := flag.String("neo4j", "neo4j://localhost:7687", "neo4j connection string")
	neo4jUser := flag.String("user", "neo4j", "neo4j user")
	neo4jPassword := flag.String("password", "1234", "neo4j password")
	stage := flag.Int("stage", -1, "which stage to run")
	dumpPath := flag.String("dump", "dump", "path of the dump to load or save")

	flag.Parse()

	if *stage == -1 {
		log.Panic("Please select which stage to run!")
	}

	importer, err := importer.NewWikidataImporter(&importer.Neo4JConfig{
		Address:  *neo4jAddress,
		Username: *neo4jUser,
		Password: *neo4jPassword,
	}, *dumpPath)
	if err != nil {
		log.Panic(err)
	}

	defer importer.Close()

	if *stage == 0 {
		err = importer.RunStage0()
		if err != nil {
			log.Panic(err)
		}
	}

	if *stage == 1 {
		err = importer.RunStage1()
		if err != nil {
			log.Panic(err)
		}
	}

	if *stage == 2 {
		err = importer.RunStage2()
		if err != nil {
			log.Panic(err)
		}
	}

	if *stage == 3 {
		err = importer.RunStage3()
		if err != nil {
			log.Panic(err)
		}
	}

	// mediawiki.ProcessWikidataDump(ctx, &config, func(c context.Context, entity mediawiki.Entity) errors.E {
	//   fmt.Printf("ID: %v\n", entity.ID)
	//   // if entity.ID == "Q103" {
	//   //   fmt.Printf("Claims: %v\n", entity.Claims["P136"][0].References[0])
	//   // }
	//   return nil
	// })
}
