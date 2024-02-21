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

import org.apache.kyuubi.KyuubiSQLException
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

    //    val tTypes =
    //      if (tableTypes == null || tableTypes.isEmpty) {
    //        Set("BASE TABLE", "SYSTEM VIEW", "VIEW")
    //      } else {
    //        tableTypes.asScala.toSet
    //      }
    //    val query = new StringBuilder(
    //      s"""
    //         |SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE,
    //         |TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH,
    //         |CREATE_TIME, UPDATE_TIME, TABLE_COLLATION, TABLE_COMMENT
    //         |FROM INFORMATION_SCHEMA.TABLES
    //         |""".stripMargin)
    //
    //    val filters = ArrayBuffer[String]()
    //    if (StringUtils.isNotBlank(catalog)) {
    //      filters += s"$TABLE_CATALOG = '$catalog'"
    //    }
    //
    //    if (StringUtils.isNotBlank(schema)) {
    //      filters += s"$TABLE_SCHEMA LIKE '$schema'"
    //    }
    //
    //    if (StringUtils.isNotBlank(tableName)) {
    //      filters += s"$TABLE_NAME LIKE '$tableName'"
    //    }
    //
    //    if (tTypes.nonEmpty) {
    //      filters += s"(${
    //        tTypes.map { tableType => s"$TABLE_TYPE = '$tableType'" }
    //          .mkString(" OR ")
    //      })"
    //    }
    //
    //    if (filters.nonEmpty) {
    //      query.append(" WHERE ")
    //      query.append(filters.mkString(" AND "))
    //    }
    //
    //    query.toString()

    throw KyuubiSQLException.featureNotSupported()
  }

  override def getColumnsQuery(
    session: Session,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String): String = {
    //    val query = new StringBuilder("show column stats ")
    //
    //    if (StringUtils.isNotEmpty(schemaName)) {
    //      query.append(s"$schemaName.")
    //    }

    throw KyuubiSQLException.featureNotSupported()
    // get column of table?
    //    ???
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new ImpalaTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new ImpalaSchemaHelper

  override def name(): String = "impala"
}
