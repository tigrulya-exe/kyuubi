/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.plugin.lineage.dispatcher.atlas

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.atlas.OpenMetadataLineageDispatcherSuite.TEST_SPARK_APP_NAME
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client.OpenMetadataClient
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{LineageDetails, OpenMetadataEntity}
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION
import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.{DEFAULT_CATALOG, DISPATCHERS, OPEN_METADATA_DATABASE_SERVICE_NAMES, OPEN_METADATA_PIPELINE_SERVICE_NAME}
import org.apache.spark.sql.{SparkListenerExtensionTest, SparkSession}

import java.util.UUID
import scala.collection.mutable

class OpenMetadataLineageDispatcherSuite extends KyuubiFunSuite with SparkListenerExtensionTest {
  private val catalogName = if (SPARK_RUNTIME_VERSION <= "3.1") {
    "org.apache.spark.sql.connector.InMemoryTableCatalog"
  } else {
    "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
  }

  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      .set(DISPATCHERS.key, "OPEN_METADATA")
      .set("spark.app.name", "2_new_local_app_name")
      .set(OPEN_METADATA_PIPELINE_SERVICE_NAME.key, "testPipelineService")
      .set(OPEN_METADATA_DATABASE_SERVICE_NAMES.key, "dbService1,hive")
  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("OpenMetadata lineage capture: insert into select sql") {


    //      new QueryExecution(spark, LocalRelation())

    withTable("test_table0") { _ =>
      spark.sql("create table test_table0(a string, b int, c int)")
      spark.sql("create table test_table1(a string, d int)")

      val queryExecution = spark.sql("insert into test_table1 select a, c as d from test_table0")
        .queryExecution


      spark.sql("insert into test_table1 select a, b + c as d from test_table0").collect()

      Thread.sleep(10000L)
      val expected = Lineage(
        List(s"$DEFAULT_CATALOG.default.test_table0"),
        List(s"$DEFAULT_CATALOG.default.test_table1"),
        List(
          (
            s"$DEFAULT_CATALOG.default.test_table1.a",
            Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
          (
            s"$DEFAULT_CATALOG.default.test_table1.d",
            Set(
              s"$DEFAULT_CATALOG.default.test_table0.b",
              s"$DEFAULT_CATALOG.default.test_table0.c"))))

      //        eventually(Timeout(5.seconds)) {
      //          assert(mockAtlasClient.getEntities != null && mockAtlasClient.getEntities.nonEmpty)
      //        }
    }
  }


  test("OpenMetadata lineage capture: insert into select sql") {
    withSparkSession(sparkSessionWithDbServices()) { spark =>


      //      new QueryExecution(spark, LocalRelation())

      withTable("test_table0") { _ =>
        spark.sql("create table test_table0(a string, b int, c int)")
        spark.sql("create table test_table1(a string, d int)")

        val queryExecution = spark.sql("insert into test_table1 select a, c as d from test_table0")
          .queryExecution

        spark.sql("insert into test_table1 select a, b + c as d from test_table0").collect()

        Thread.sleep(10000L)
        val expected = Lineage(
          List(s"$DEFAULT_CATALOG.default.test_table0"),
          List(s"$DEFAULT_CATALOG.default.test_table1"),
          List(
            (
              s"$DEFAULT_CATALOG.default.test_table1.a",
              Set(s"$DEFAULT_CATALOG.default.test_table0.a")),
            (
              s"$DEFAULT_CATALOG.default.test_table1.d",
              Set(
                s"$DEFAULT_CATALOG.default.test_table0.b",
                s"$DEFAULT_CATALOG.default.test_table0.c"))))

        //        eventually(Timeout(5.seconds)) {
        //          assert(mockAtlasClient.getEntities != null && mockAtlasClient.getEntities.nonEmpty)
        //        }
      }
    }
  }

  private def sparkSessionWithDbServices(dbServices: String*): SparkSession = {
    sparkSessionBuilder
      //      .config(
      //        "spark.sql.queryExecutionListeners",
      //        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      //      .config(OPEN_METADATA_SERVER_ADDRESS.key, "http://some_address:8888")
      .config(DISPATCHERS.key, "OPEN_METADATA")
      .config(OPEN_METADATA_DATABASE_SERVICE_NAMES.key, dbServices.mkString(","))
      .config("spark.app.name", TEST_SPARK_APP_NAME)
      .getOrCreate()
  }

  class MockOpenMetadataClient(tableEntities: Map[String, OpenMetadataEntity])
    extends OpenMetadataClient {
    val pipelineServices: mutable.Map[String, OpenMetadataEntity] = mutable.Map()
    val pipelines: mutable.Map[String, OpenMetadataEntity] = mutable.Map()
    val lineages: mutable.Map[LineageKey, LineageDetails] = mutable.Map()

    override def createPipelineServiceIfNotExists(pipelineService: String): OpenMetadataEntity = {
      pipelineServices.getOrElseUpdate(
        pipelineService,
        entity(pipelineService, "pipelineService"))
    }

    override def createPipelineIfNotExists(
      pipelineService: String,
      pipeline: String,
      description: String): OpenMetadataEntity = {

      pipelineServices.getOrElseUpdate(
        pipelineService,
        entity(pipelineService, "pipeline"))
    }

    override def getTableEntity(fullyQualifiedNameTemplate: String): Option[OpenMetadataEntity] = {
      tableEntities.get(fullyQualifiedNameTemplate)
    }

    override def addLineage(
      from: OpenMetadataEntity,
      to: OpenMetadataEntity,
      lineageDetails: LineageDetails): Unit = {
      lineages(LineageKey(from.fullyQualifiedName, to.fullyQualifiedName)) = lineageDetails
    }

    private def entity(name: String, entityType: String) =
      OpenMetadataEntity(UUID.randomUUID(), name, entityType)
  }

  case class LineageKey(from: String, to: String)
}

object OpenMetadataLineageDispatcherSuite {
  val TEST_SPARK_APP_NAME = "open_metadata_test_app"
}
