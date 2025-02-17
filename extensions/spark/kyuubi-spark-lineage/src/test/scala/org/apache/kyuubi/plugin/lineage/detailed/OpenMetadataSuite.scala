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
      .set("spark.app.name", "2_new_local_app_name")
      .set("spark.kyuubi.plugin.lineage.openmetadata.server",
        "http://hdfs-kafka-tiered-storage-test-1.ru-central1-a.internal:8585/")
// .set("spark.kyuubi.plugin.lineage.openmetadata.databaseServiceNames", "NewHive,mysql_sample")
      .set(
        "spark.kyuubi.plugin.lineage.openmetadata.jwt",
        "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLC" +
          "JhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFk" +
          "YXRhLm9yZyIsInN1YiI6ImRhdGFpbnNpZ2h0c2FwcGxpY2F0aW9uYm90Iiwi" +
          "cm9sZXMiOltudWxsXSwiZW1haWwiOiJkYXRhaW5zaWdodHNhcHBsaWNhdGlv" +
          "bmJvdEBvcGVubWV0YWRhdGEub3JnIiwiaXNCb3QiOnRydWUsInRva2VuVHlw" +
          "ZSI6IkJPVCIsImlhdCI6MTc0MDM5MDQyNywiZXhwIjpudWxsfQ.biKQ82BMz" +
          "IQsbZc2z-Vl8erObAkBkEJCtmhV9xblW3XLTFonk2wSv6HCKAIVWHnHIPFnR" +
          "NG3O8-IZ4piOWmzhQ2AD0nqVdWkzXmuZSg7YBccI0jxFHTYa9WklYt6gb8yF" +
          "qgTcCutefUtNQzwut0u2Bt0o_9K4Ej2O_eBOiAHDCq3fbpjiFgyQxZHe3vGx" +
          "jG1wPGItMie9YJYWIaCaL60GDqCeCmmMOq9_L6S9z9fKK1e5p5pqUcIhPHq1" +
          "srunTORrNqhXKoGyQQZhroAECUTlTn1qz0bI-lsGIksMd_a3A9WVkDZJ0ljr" +
          "QXrwdq632xQKkR6POuC5taKIsp_0D8fiQ")
      .set("spark.kyuubi.plugin.lineage.openmetadata.pipelineServiceName", "HIVEEEEEE")
  }

  test("lineage event was written to HDFS") {
    withTable("Users") { _ =>
      withTable("Categories") { _ =>
        spark.sql("create table src1(str_col string, other_col int, another_col int)")
        spark.sql("create table src2(str_col2 string, some_col int, another_col int)")
        spark.sql("create table simple_join_dest(" +
          "src1_col string, src2_col string, another_col int)")

//        val tableDirectory = getClass.getResource("/").getPath + "table_directory"
//        val directory = File(tableDirectory).createDirectory()
//        val sqlQuery = s"""
//                                     |INSERT OVERWRITE DIRECTORY '${directory.path}'
//                                     |USING parquet
//                                     |SELECT * FROM Users""".stripMargin

        val sqlQuery = "insert into simple_join_dest select src1.str_col, src2.str_col2, " +
          "src1.other_col from src1 join src2 on src1.str_col = src2.str_col2;"
//        val sqlQuery = "insert into newtable3 select column1 from newtable5"
        spark.sql(sqlQuery).collect()


        spark.sql("create table newtable3(column1 varchar(100))")
        spark.sql("create table newtable4(column1 varchar(100))")

        val sqlQuery2 = "insert into newtable3 select column1 from newtable4"
        spark.sql(sqlQuery2).collect()

        Thread.sleep(5000L)
      }
    }
  }
}
