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

import java.util.UUID

import org.openmetadata.client.api._
import org.openmetadata.client.model._
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig
import org.openmetadata.schema.services.connections.metadata.{AuthProvider, OpenMetadataConnection}

import org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.model.{OpenMetadataEntity, OpenMetadataPipeline}

class RestOpenMetadataClient(
  serverAddress: String,
  jwt: String = null
) extends OpenMetadataClient {

  private val openMetadataGateway: OpenMetadataGateway = {
    val connection = new OpenMetadataConnection()
      .withHostPort(serverAddress)
      .withApiVersion("v1")

    if (jwt != null) {
      connection.withAuthProvider(AuthProvider.OPENMETADATA)
        .withSecurityConfig(new OpenMetadataJWTClientConfig().withJwtToken(jwt))
    }

    new OpenMetadataGateway(connection)
  }

  private val searchApi: SearchApi = openMetadataGateway.buildClient(classOf[SearchApi])
  private val lineageApi: LineageApi = openMetadataGateway.buildClient(classOf[LineageApi])
  private val pipelinesApi: PipelinesApi = openMetadataGateway.buildClient(classOf[PipelinesApi])
  private val pipelineServiceApi: PipelineServicesApi =
    openMetadataGateway.buildClient(classOf[PipelineServicesApi])

  override def getTableEntity(fullyQualifiedNameTemplate: String): Option[OpenMetadataEntity] = {
    val searchResult = searchApi.searchEntitiesWithSpecificFieldAndValue(
      "fullyQualifiedName",
      fullyQualifiedNameTemplate,
      "table_search_index"
    )

    val searchHits = searchResult.getHits

    if (searchHits.getTotalHits.getValue == 0) {
      None
    } else {
      val entityMap = searchHits.getHits
        .get(0)
        .getSourceAsMap
      Some(OpenMetadataEntity(
        UUID.fromString(entityMap.get("id").asInstanceOf[String]),
        entityMap.get("fullyQualifiedName").asInstanceOf[String],
        entityMap.get("entityType").asInstanceOf[String]
      ))
    }
  }

  override def addLineage(
    pipeline: OpenMetadataPipeline,
    from: OpenMetadataEntity,
    to: OpenMetadataEntity): Unit = {
    val lineageDetails = new LineageDetails()
      .pipeline(toEntityRef(pipeline))

    val entitiesEdge = new EntitiesEdge()
      .lineageDetails(lineageDetails)
      .fromEntity(toEntityRef(from))
      .toEntity(toEntityRef(to))

    val request = new AddLineage()
      .edge(entitiesEdge)

    lineageApi.addLineageEdge(request)
  }

  override def createPipelineServiceIfNotExists(pipelineService: String): OpenMetadataEntity = {
    val createPipelineRequest = new CreatePipelineService()
      .name(pipelineService)
      .serviceType(CreatePipelineService.ServiceTypeEnum.SPARK)
    val pipelineServiceEntity =
      pipelineServiceApi.createOrUpdatePipelineService(createPipelineRequest)
    OpenMetadataEntity(
      pipelineServiceEntity.getId,
      pipelineServiceEntity.getFullyQualifiedName,
      pipelineServiceEntity.getServiceType.toString
    )
  }

  override def createPipelineIfNotExists(
    pipelineServiceId: UUID, pipeline: String): OpenMetadataPipeline = {
    val createPipelineRequest = new CreatePipeline()
      .service(pipelineServiceId.toString)
      .name(pipeline)

    val pipelineEntity = pipelinesApi.createPipeline(createPipelineRequest)
    new OpenMetadataPipeline(pipelineEntity.getId)
  }

  private def toEntityRef(entity: OpenMetadataEntity): EntityReference = {
    val entityRef = new EntityReference()
    entityRef.id(entity.id)
    entityRef.`type`(entity.entityType)
    entityRef
  }
}
