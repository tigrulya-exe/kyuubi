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

import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{EntityColumnLineage, LineageDetails, OpenMetadataEntity}
import org.apache.kyuubi.plugin.lineage.{ColumnLineage, Lineage}
import org.apache.spark.sql.execution.QueryExecution

class OpenMetadataLineageLogger(
  val openMetadataClient: OpenMetadataClient,
  val databaseServiceNames: Seq[String],
  pipelineServiceName: String) {
  private val pipelineService = openMetadataClient
    .createPipelineServiceIfNotExists(pipelineServiceName)

  def log(execution: QueryExecution, lineage: Lineage): Unit = {
    val inputTableEntities = getTableNameToEntityMapping(lineage.inputTables)
    val outputTableEntities = getTableNameToEntityMapping(lineage.outputTables)

    val pipeline = openMetadataClient.createPipelineIfNotExists(
      pipelineService.fullyQualifiedName, getPipelineName(execution))

    val entityColumnLineage = buildEntityColumnLineage(
      inputTableEntities,
      outputTableEntities,
      lineage.columnLineage
    )

    for (fromTable <- inputTableEntities.values;
         toTable <- outputTableEntities.values) {
      val lineageDetails = LineageDetails(
        pipeline.toReference,
        execution.toString(),
        execution.logical.origin.sqlText.orNull,
        getEntityColumnLineage(fromTable, toTable, entityColumnLineage)
      )

      openMetadataClient.addLineage(fromTable, toTable, lineageDetails)
    }
  }

  private def getEntityColumnLineage(
    inputEntity: OpenMetadataEntity,
    outputEntity: OpenMetadataEntity,
    entityColumnLineage: Map[String, Seq[EntityColumnLineage]]
  ): Seq[EntityColumnLineage] = {
    entityColumnLineage.get(outputEntity.fullyQualifiedName)
      .map { lineages =>
        lineages.map { lineage =>
          EntityColumnLineage(
            lineage.toColumn,
            lineage.fromColumns
              .filter(inputEntity.fullyQualifiedName == extractTableName(_))
          )
        }
      }
      .getOrElse {
        throw new RuntimeException("TODO")
      }
  }

  private def buildEntityColumnLineage(
    inputEntities: Map[String, OpenMetadataEntity],
    outputEntities: Map[String, OpenMetadataEntity],
    columnLineage: List[ColumnLineage]
  ): Map[String, Seq[EntityColumnLineage]] = {
    columnLineage.map {
      lineage => {
        val outputEntityColumn = mapColumnName(lineage.column, outputEntities)
        val inputEntityColumns = lineage.originalColumns
          .map(mapColumnName(_, inputEntities))
          .toSeq

        EntityColumnLineage(outputEntityColumn, inputEntityColumns)
      }
    }.groupBy { lineage => extractTableName(lineage.toColumn) }
  }

  private def extractTableName(fullColumnName: String): String =
    splitColumnName(fullColumnName)._1

  private def splitColumnName(fullColumnName: String): (String, String) = {
    fullColumnName.lastIndexOf('.') match {
      case -1 => throw new RuntimeException("TODO")
      case idx => (fullColumnName.substring(0, idx), fullColumnName.substring(idx + 1))
    }
  }

  private def mapColumnName(fullColumnName: String,
                            entityMappings: Map[String, OpenMetadataEntity]): String = {
    val (fullTableName, columnName) = splitColumnName(fullColumnName)

    entityMappings.get(fullTableName)
      .map(_.fullyQualifiedName)
      .map { fqn => s"$fqn.$columnName" }
      .getOrElse {
        throw new RuntimeException("TODO")
      }
  }

  private def getTableNameToEntityMapping(
    sparkTableNames: List[String]): Map[String, OpenMetadataEntity] = {
    sparkTableNames.map { table =>
      (table, getTableEntity(table))
    }.toMap
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
    val prefix = if (prefixes.isEmpty) {
      ""
    } else {
      prefixes.mkString("", ".", ".")
    }
    s"$prefix*${tableName.split("\\.").last}"
  }

  private def getPipelineName(execution: QueryExecution): String = {
    execution.sparkSession.conf.get("spark.app.name") + "_" + execution.id
  }
}
