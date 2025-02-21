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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import feign.Feign
import feign.Logger.Level
import feign.jackson.{JacksonDecoder, JacksonEncoder}
import feign.okhttp.OkHttpClient
import feign.slf4j.Slf4jLogger

class OpenMetadataClientFactory(
    serverAddress: String,
    authTokenProvider: AuthenticationTokenProvider) {

  private val basePath = if (serverAddress.endsWith("/")) {
    serverAddress
  } else {
    serverAddress + "/"
  }

  private lazy val objectMapper = new ObjectMapper()
    .enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING)
    .enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING)
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .disable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule)
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)

  private lazy val feignBuilder: Feign.Builder = Feign.builder
    .encoder(new JacksonEncoder(objectMapper))
    .decoder(new JacksonDecoder(objectMapper))
    .requestInterceptor(new BearerAuthInterceptor(authTokenProvider))
    .logLevel(Level.FULL)
    .logger(new Slf4jLogger)
    .client(new OkHttpClient)

  def buildClient(): OpenMetadataApi = {
    feignBuilder.target(classOf[OpenMetadataApi], basePath)
  }
}
