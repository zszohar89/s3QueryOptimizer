package main

import (
	"testing"
)

func TestParseSqlPlan(t *testing.T) {
	var sqlPlanEvent = `
{
"Event" : "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
"executionId" : 0,
"description" : "parquet at PerformanceTest.scala:169",
"physicalPlanDescription" : "physicalPlanDescription-test"
}
`

 expected := SparkQueryPlan{"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",0,
 	"parquet at PerformanceTest.scala:169", "physicalPlanDescription-test"}

 parseSqlPlanOutPut, err := unmarshalSqlExecutionPlanEvent(sqlPlanEvent)
 if err == nil {
 	if expected != parseSqlPlanOutPut {
 	t.Errorf("function returned  wrong error : got %v want %v", parseSqlPlanOutPut, expected)
 	}
 } else {
	 t.Errorf("something bad happend error is %s", err.Error())
 }


}


func TestGetPhysicalPlan(t *testing.T) {
	fullPlan := "== Parsed Logical Plan ==\nInsertIntoHadoopFsRelationCommand s3a://af-redshift-staging/zohar-testing/s3a:/af-eu-west-1-stg-data-lake/organicinappevents/dt=2019-09-23/tm=10, false, Parquet, Map(path -> s3a://af-redshift-staging/zohar-testing/s3a://af-eu-west-1-stg-data-lake/organicinappevents/dt=2019-09-23/tm=10), Overwrite, [device_install_time, date_1, device_type, app_id, app_name, bundle_id, install_time, install_hour_millis, media_source, is_organic, is_retargeting, user_agent, country, ltv_country, state, geo_region, geo_state, city, postal_code, dma, _d_m_a, ip, ip_address, wifi, ... 81 more fields]== Physical Plan ==\nExecute InsertIntoHadoopFsRelationCommand InsertIntoHadoopFsRelationCommand"
	expected := "== Physical Plan ==\nExecute InsertIntoHadoopFsRelationCommand InsertIntoHadoopFsRelationCommand"
	physicalPlanOutCome := getPhysicalPlan(fullPlan)
	if physicalPlanOutCome != expected {
		t.Errorf("function returned  wrong error : got %v want %v", physicalPlanOutCome, expected)
	}
}