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
package org.apache.kyuubi.engine.jdbc.dialect

import org.apache.commons.lang3.StringUtils
import org.apache.kyuubi.engine.jdbc.impala.{ImpalaSchemaHelper, ImpalaTRowSetGenerator}
import org.apache.kyuubi.engine.jdbc.schema.{JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.session.Session

import java.util

class ImpalaDialect extends JdbcDialect {

  override def getTablesQuery(
    catalog: String,
    schema: String,
    tableName: String,
    tableTypes: util.List[String]): String = {
    val query = new StringBuilder("show tables ")

    if (StringUtils.isNotEmpty(schema) && !isWildcard(schema)) {
      query.append(s"in ${toImpalaRegex(schema)} ")
    }

    if (StringUtils.isNotEmpty(tableName)) {
      query.append(s"like '${toImpalaRegex(tableName)}'")
    }

    // todo handle tableTypes
    query.toString()
  }

  override def getColumnsQuery(
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String): String = {
    val query = new StringBuilder("show column stats ")

    if (StringUtils.isNotEmpty(schemaName) && !isWildcard(schemaName)) {
      query.append(s"$schemaName.")
    }

    query.append(tableName)
    query.toString()
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new ImpalaTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new ImpalaSchemaHelper

  override def name(): String = "impala"

  private def isWildcard(pattern: String): Boolean = {
    pattern == "%" || pattern == "*"
  }

  private def toImpalaRegex(pattern: String): String = {
    pattern.replace("%", "*")
  }
}
