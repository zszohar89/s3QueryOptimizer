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
	Details string
	PhysicalPlanDescription string
	SparkPlanInfo string
	Time string
}

func readLines(path string) ([]string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Print("got an error when trying to load data from file")
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

func parsePhysicalPlan(PhysicalPlanDescription string) {
	strings.Split(string(data), "\n")
}


func main() {
	splitData, err := readLines("application_1570695864101_4014_1")
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
