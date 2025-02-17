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

package org.apache.kyuubi.plugin.lineage.detailed.openmetadata

import org.apache.kyuubi.plugin.lineage.detailed.openmetadata.model.OpenMetadataEntity
import org.openmetadata.client.api.{LineageApi, SearchApi, TablesApi}
import org.openmetadata.client.gateway.OpenMetadata
import org.openmetadata.client.model.{AddLineage, EntitiesEdge, EntityReference}
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig
import org.openmetadata.schema.services.connections.metadata.{AuthProvider, OpenMetadataConnection}

import java.util.UUID

class RestOpenMetadataClient(
  serverAddress: String,
  jwt: String = null
) extends OpenMetadataClient {

  private val openMetadataGateway: OpenMetadata = {
    val connection = new OpenMetadataConnection()
    connection.setHostPort(serverAddress)
    connection.setApiVersion("v1")

    if (jwt != null) {
      connection.setAuthProvider(AuthProvider.OPENMETADATA)
      val securityConf = new OpenMetadataJWTClientConfig()
      securityConf.setJwtToken(jwt)
      connection.setSecurityConfig(securityConf)
    }

    new OpenMetadata(connection)
  }

  private val tablesApi: TablesApi = openMetadataGateway.buildClient(classOf[TablesApi])
  private val searchApi: SearchApi = openMetadataGateway.buildClient(classOf[TablesApi])
  private val lineageApi: LineageApi = openMetadataGateway.buildClient(classOf[LineageApi])

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

  override def addLineage(from: OpenMetadataEntity, to: OpenMetadataEntity): Unit = {
    val request = new AddLineage()
    val entitiesEdge = new EntitiesEdge

    entitiesEdge.fromEntity(toEntityRef(from))
    entitiesEdge.toEntity(toEntityRef(to))

    request.setEdge(entitiesEdge)
    lineageApi.addLineageEdge(request)
  }

  private def toEntityRef(entity: OpenMetadataEntity): EntityReference = {
    val entityRef = new EntityReference()
    entityRef.id(entity.id)
    entityRef.`type`(entity.entityType)
    entityRef
  }
}
