/*
 *  Copyright 2021 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kyuubi.plugin.lineage.dispatcher.openmetadata.client

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import feign.Feign
import feign.Logger.Level
import feign.form.FormEncoder
import feign.jackson.{JacksonDecoder, JacksonEncoder}
import feign.okhttp.OkHttpClient
import feign.slf4j.Slf4jLogger
import org.openapitools.jackson.nullable.JsonNullableModule
import org.openmetadata.client.security.factory.AuthenticationProviderFactory
import org.openmetadata.client.{ApiClient, RFC3339DateFormat}
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection

class OpenMetadataGateway(config: OpenMetadataConnection) {

  private lazy val objectMapper = new ObjectMapper()
    .enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
    .enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .setDateFormat(new RFC3339DateFormat)
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule)
    .registerModule(new JsonNullableModule)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private val apiClient: ApiClient = {
    val client = new ApiClient()
      .setFeignBuilder(feignBuilder)
      .setBasePath(config.getHostPort + "/")

    client.addAuthorization("oauth", new AuthenticationProviderFactory().getAuthProvider(config))

    client
  }

  def buildClient[T](clientClass: Class[T]): T = {
    apiClient.getFeignBuilder.target(clientClass, apiClient.getBasePath)
  }

  private def feignBuilder = Feign.builder
    .encoder(new FormEncoder(new JacksonEncoder(objectMapper)))
    .decoder(new JacksonDecoder(objectMapper))
    .logLevel(Level.FULL)
    .logger(new Slf4jLogger)
    //    .logger(new ErrorLogger)
    .client(new OkHttpClient)
}
