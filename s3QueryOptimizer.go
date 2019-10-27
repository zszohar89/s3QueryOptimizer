package main

import (
	"io/ioutil"
	"log"
	"strings"
)


func readLines(path string) ([]string, error) {
	data, err := ioutil.ReadFile(path)
	if data != nil {
		return nil, err
	} else {
		log.Print(strings.Split(string(data), "\n"))
		return strings.Split(string(data), "\n"), nil
	}

}

func extractSQLExecutionPlanEvents(data []string) []string {
	var sqlPlans []string
	for _, event := range data {
		if strings.Contains(event, "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart") {
			sqlPlans = append(sqlPlans, event)
			log.Print(sqlPlans)
		}
	}
	return sqlPlans
}

func parseSqlPlan(sqlPlan string) {

}


func main() {
	splitData, err := readLines("application_1570695864101_4014_1")
	log.Print(splitData)
	if err != nil {
		log.Fatal(err)
	} else {
	sqlPlans := extractSQLExecutionPlanEvents(splitData)
		if len(sqlPlans) != 0 {
			for _, sqlPlan := range sqlPlans {
				parseSqlPlan(sqlPlan)
			}

		} else {
			log.Print("no sql plan detected in data")
		}
	}
}
