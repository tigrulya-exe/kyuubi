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

package org.apache.kyuubi.plugin.lineage.detailed

import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf.DISPATCHERS
import org.apache.spark.sql.SparkListenerExtensionTest
import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION

import scala.reflect.io.File

class OpenMetadataSuite extends KyuubiFunSuite
  with SparkListenerExtensionTest {

  val catalogName: String = if (SPARK_RUNTIME_VERSION <= "3.1") {
    "org.apache.spark.sql.connector.InMemoryTableCatalog"
  } else {
    "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"
  }

  override protected val catalogImpl: String = "hive"

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.sql.catalog.v2_catalog", catalogName)
      .set(
        "spark.sql.queryExecutionListeners",
        "org.apache.kyuubi.plugin.lineage.SparkOperationLineageQueryExecutionListener")
      .set(DISPATCHERS.key, "OPEN_METADATA")
      .set("spark.app.name", "test_spark_app_name")
      .set("spark.kyuubi.plugin.lineage.openmetadata.server",
        "http://localhost:8585/api")
      .set("spark.kyuubi.plugin.lineage.openmetadata.jwt",
        "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1" +
          "NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImRhdG" +
          "FpbnNpZ2h0c2FwcGxpY2F0aW9uYm90Iiwicm9sZXMiOltudWxsXSwiZW1haWwiOiJkYXRh" +
          "aW5zaWdodHNhcHBsaWNhdGlvbmJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydW" +
          "UsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTczOTc5NDU3MSwiZXhwIjpudWxsfQ.C8eLTd" +
          "L7dlwqb2akNTbbMY0xnzZd3oqfnV8Bwg6kC48PufFfv0MSvv27Zhem6Fw7nIVdPFHpEz6yxd2" +
          "sfwyb1mA2Vj_bK1ayvZCR-AsJQQRZksxHYlWZ_q31E5mRqLRZQEpoTGgHyg8C1D6kttc4x6" +
          "JMO6rIpjZvn3Zu-ytwtciEBZLnnQB5w2J-Xo9P0MYLwiYScGAjPdDj3q4jeSFPAn5FNBxm0odi" +
          "r-DMejweCywyzx_cW2cDGlsO1KVIUFJr9hnZYl69ZvdwXJELcWk8y7PR4RVgLjVvNJ5_ol1hdef1" +
          "lbfFYzuzXlvS-x22rW_l3HDB09OOlcv5i-FkZU_6yA")
      .set("spark.kyuubi.plugin.lineage.openmetadata.pipelineServiceName",
        "BRR_CHACHABRRRSparkOnKyuubiServiceQ")
  }

  test("lineage event was written to HDFS") {
    withTable("Users") { _ =>
      withTable("Categories") { _ =>
        spark.sql("create table Users(user_id string, email string)")
        spark.sql("create table Categories(category_id string, name string)")

        val tableDirectory = getClass.getResource("/").getPath + "table_directory"
        val directory = File(tableDirectory).createDirectory()
        val sqlQuery = s"""
                                     |INSERT OVERWRITE DIRECTORY '${directory.path}'
                                     |USING parquet
                                     |SELECT * FROM Users""".stripMargin

//        val sqlQuery = "insert into Categories select user_id, email from Users"
        spark.sql(sqlQuery).collect()
        Thread.sleep(5000L)
      }
    }
  }
}
