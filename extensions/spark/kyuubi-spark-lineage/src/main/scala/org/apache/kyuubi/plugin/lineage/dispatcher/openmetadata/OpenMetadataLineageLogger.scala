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

package org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata

import org.apache.spark.sql.execution.QueryExecution

import org.apache.kyuubi.plugin.lineage.Lineage
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.OpenMetadataEntity

class OpenMetadataLineageLogger(
  val openMetadataClient: OpenMetadataClient,
  val databaseServiceNames: Seq[String],
  pipelineServiceName: String) {
  private val pipelineServiceId = openMetadataClient
    .createPipelineServiceIfNotExists(pipelineServiceName).id

  def log(execution: QueryExecution, lineage: Lineage): Unit = {
    val inputTableEntities = lineage.inputTables.map(getTableEntity)
    val outputTableEntities = lineage.outputTables.map(getTableEntity)

    val pipeline = openMetadataClient.createPipelineIfNotExists(
      pipelineServiceId, getPipelineName(execution))

    for (fromTable <- inputTableEntities; toTable <- outputTableEntities) {
      openMetadataClient.addLineage(pipeline, fromTable, toTable)
    }
  }

  private def getTableEntity(tableName: String): OpenMetadataEntity = {
    val maybeTableEntity = if (databaseServiceNames.isEmpty) {
      searchTableEntityGlobally(tableName)
    } else {
      searchTableEntityInServices(tableName)
    }

    maybeTableEntity.getOrElse {
      throw new IllegalArgumentException(s"OpenMetadata Entity for table $tableName not found")
    }
  }

  private def searchTableEntityGlobally(tableName: String): Option[OpenMetadataEntity] = {
    val searchPattern = getTableEntityPattern(tableName)
    openMetadataClient.getTableEntity(searchPattern)
  }

  private def searchTableEntityInServices(tableName: String): Option[OpenMetadataEntity] = {
    databaseServiceNames
      .view
      .map { dbService =>
        val searchPattern = getTableEntityPattern(tableName, dbService)
        openMetadataClient.getTableEntity(searchPattern)
      }
      .find(_.isDefined)
      .flatten
  }

  private def getTableEntityPattern(tableName: String, prefixes: String*): String = {
    val prefix = prefixes.mkString("", ".", ".")
    s"$prefix*$tableName"
  }

  private def getPipelineName(execution: QueryExecution): String = {
    execution.sparkSession.conf.get("spark.app.name")
  }
}
