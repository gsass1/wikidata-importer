# Wikidata Importer 
#### GDBS Gruppe 3
* Gian Saß
* Felix Ifland

## Setup
### Prerequisites
To build the Importer the following software is needed
* Go >= 1.18
* Make
No dependencies are required to execute the precompiled binary

To build the tool run:
```bash
go mod tidy   # To install dependencies
make          # To build the tool
```
The executable ban now be found within the `./bin/` directory

## Usage
To use the Importer simply call it's executable:
```bash
./wikidata-importer --help
```
The following parameters are required:
```
Usage of ./wikidata-importer:
  -dump string
        path of the dump to load or save (default "dump")
  -neo4j string
        neo4j connection string (default "neo4j://localhost:7687")
  -password string
        neo4j password (default "1234")
  -stage int
        which stage to run (default -1)
  -user string
        neo4j user (default "neo4j")
```
An average call might therefore look like:
```bash
./wikidata-importer -dump ./data/latest-all.json.gz -neo4j neo4j://localhost:7687 -user neo4j -password 1234
```