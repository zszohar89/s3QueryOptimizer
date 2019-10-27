package main

import "testing"

func TestParseSqlPlan(t *testing.T) {
	var sqlPlanEvent = `
{"Event" : "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
"executionId" : 0,
"description" : "parquet at PerformanceTest.scala:169",
"details" : "test",
"physicalPlanDescription" : "physicalPlanDescription-test",
"sparkPlanInfo" : "sparkPlanInfo-test",
"time" : "1234"}
`

 expected := SparkQueryPlan{"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",0,
 	"parquet at PerformanceTest.scala:169", "test", "physicalPlanDescription-test", "sparkPlanInfo-test",
 "1234"}

 parseSqlPlanOutPut, err := unmarshalSqlExecutionPlanEvent(sqlPlanEvent)
 if err == nil {
 	if expected != parseSqlPlanOutPut{
 	t.Errorf("function returned  wrong error : got %v want %v", parseSqlPlanOutPut, expected)
 	}
 } else {
	 t.Errorf("something bad happend error is %s", err.Error())
 }


}