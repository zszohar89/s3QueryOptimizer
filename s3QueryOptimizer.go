package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
)

type SparkQueryPlan struct {
	Event string
	ExecutionId int32
	Description string
	PhysicalPlanDescription string
}

func readLines(path string) ([]string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("got an error when trying to load data from file path %v ", path )
		return nil, err
	} else {
		return strings.Split(string(data), "\n"), nil
	}

}

func extractSQLExecutionPlanEvents(data []string) []string {
	var sqlPlans []string
	for _, event := range data {
		if strings.Contains(event, "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart") {
			sqlPlans = append(sqlPlans, event)
		}
	}
	return sqlPlans
}

func unmarshalSqlExecutionPlanEvent(sqlPlan string) (SparkQueryPlan, error) {
	var sparkQueryPlan SparkQueryPlan
	err := json.Unmarshal([]byte(sqlPlan), &sparkQueryPlan)
	if err == nil {
		return sparkQueryPlan, nil
	} else {
		return sparkQueryPlan, err
	}
}

func getPhysicalPlan(physicalPlanDescription string) string  {
	physicalPlanStartsFromIndex := strings.Index(physicalPlanDescription, "== Physical Plan ==")
	if physicalPlanStartsFromIndex != -1 {
		return physicalPlanDescription[physicalPlanStartsFromIndex:]
	} else {
		log.Print("no physical plan found in the Physical plan description ")
		return ""
	}
}

//TODO check for what is InMemoryFileIndex - does this mean the data was persisted
func extractS3ReadOperations(physicalPlan string) string {
	physicalPlanStages := strings.Split(physicalPlan, "\n")
	for _, data := range physicalPlanStages {
		if strings.Contains(data, "s3a") &&
			strings.Contains(data, "Format: Parquet") &&
			!strings.Contains(data, "InsertIntoHadoopFsRelationCommand") {
			log.Print(data)
		}
	}
	return ""
}


func parsePhysicalPlan(physicalPlanDescription string) {
	physicalPlan := getPhysicalPlan(physicalPlanDescription)
	if physicalPlan != "" {
		extractS3ReadOperations(physicalPlan)
	}

}

func getData(pathToFile string) {
	splitData, err := readLines(pathToFile)
	if err != nil {
		log.Fatal(err)
	} else {
		sqlPlans := extractSQLExecutionPlanEvents(splitData)
		if len(sqlPlans) != 0 {
			for _, sqlPlan := range sqlPlans {
				sparkQueryPlan, err := unmarshalSqlExecutionPlanEvent(sqlPlan)
				if err == nil {
					parsePhysicalPlan(sparkQueryPlan.PhysicalPlanDescription)
				}
			}

		} else {
			log.Print("no sql plan detected in data")
		}
	}
}

func main() {
	fileNames := []string{"resources/application_1570695864101_4014_1","resources/application_1571960778295_8341_1","resources/application_1572064184415_1990_1"}
	for _,fileName := range fileNames {
		getData(fileName)
	}
}
