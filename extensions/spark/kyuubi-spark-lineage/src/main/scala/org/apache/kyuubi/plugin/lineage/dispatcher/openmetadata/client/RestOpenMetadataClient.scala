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

package org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client

import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client.RestOpenMetadataClient._
import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{AddLineageRequest, LineageDetails, LineageEdge, OpenMetadataEntity}
import org.openmetadata.client.model._
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig
import org.openmetadata.schema.services.connections.metadata.{AuthProvider, OpenMetadataConnection}

class RestOpenMetadataClient(
  serverAddress: String,
  jwt: String = null
) extends OpenMetadataClient {

  private lazy val openMetadataGateway: OpenMetadataGateway = {
    val connection = new OpenMetadataConnection()
      .withHostPort(serverAddress)
      .withApiVersion(API_VERSION)

    if (jwt != null) {
      connection.withAuthProvider(AuthProvider.OPENMETADATA)
        .withSecurityConfig(new OpenMetadataJWTClientConfig().withJwtToken(jwt))
    }
    new OpenMetadataGateway(connection)
  }

  private lazy val openMetadataApi: OpenMetadataApi =
    openMetadataGateway.buildClient(classOf[OpenMetadataApi])

  override def getTableEntity(fullyQualifiedNameTemplate: String): Option[OpenMetadataEntity] = {
    val searchResult = openMetadataApi.searchEntitiesWithSpecificFieldAndValue(
      TABLE_ENTITY_FQN_FIELD,
      fullyQualifiedNameTemplate,
      TABLE_ENTITY_SEARCH_INDEX
    )

    searchResult.hits.hits
      .headOption
      .map(_.entity)
  }

  override def addLineage(
    from: OpenMetadataEntity,
    to: OpenMetadataEntity,
    lineageDetails: LineageDetails): Unit = {
    val request = AddLineageRequest(
      LineageEdge(from.toReference, to.toReference, lineageDetails)
    )
    openMetadataApi.addLineageEdge(request)
  }

  override def createPipelineServiceIfNotExists(pipelineService: String): OpenMetadataEntity = {
    val createPipelineRequest = new CreatePipelineService()
      .name(pipelineService)
      .serviceType(CreatePipelineService.ServiceTypeEnum.SPARK)
    openMetadataApi.createOrUpdatePipelineService(createPipelineRequest)
      .withType(PIPELINE_SERVICE_ENTITY_TYPE)
  }

  override def createPipelineIfNotExists(
    pipelineService: String, pipeline: String): OpenMetadataEntity = {
    val createPipelineRequest = new CreatePipeline()
      .service(pipelineService)
      .name(pipeline)

    openMetadataApi.createOrUpdatePipeline(createPipelineRequest)
      .withType(PIPELINE_ENTITY_TYPE)
  }
}

object RestOpenMetadataClient {
  private val API_VERSION = "v1"

  private val TABLE_ENTITY_FQN_FIELD = "fullyQualifiedName"
  private val TABLE_ENTITY_SEARCH_INDEX = "table_search_index"

  private val PIPELINE_ENTITY_TYPE = "pipeline"
  private val PIPELINE_SERVICE_ENTITY_TYPE = "pipelineService"
}