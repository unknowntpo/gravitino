/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.mysql.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for MySQL. */
public class MysqlTypeConverter extends JdbcTypeConverter {

  static final String BIT = "bit";
  static final String TINYINT = "tinyint";
  static final String TINYINT_UNSIGNED = "tinyint unsigned";
  static final String SMALLINT = "smallint";
  static final String SMALLINT_UNSIGNED = "smallint unsigned";
  static final String INT = "int";
  static final String INT_UNSIGNED = "int unsigned";
  static final String BIGINT = "bigint";
  static final String BIGINT_UNSIGNED = "bigint unsigned";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String DECIMAL = "decimal";
  static final String DECIMAL_UNSIGNED = "decimal unsigned";
  static final String CHAR = "char";
  static final String BINARY = "binary";
  static final String DATETIME = "datetime";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    switch (typeBean.getTypeName().toLowerCase()) {
      case BIT:
        if (typeBean.getColumnSize() == null || typeBean.getColumnSize() == 1) {
          return Types.BooleanType.get();
        }
        return Types.BinaryType.get();
      case TINYINT:
        return Types.ByteType.get();
      case TINYINT_UNSIGNED:
        return Types.ByteType.unsigned();
      case SMALLINT:
        return Types.ShortType.get();
      case SMALLINT_UNSIGNED:
        return Types.ShortType.unsigned();
      case INT:
        return Types.IntegerType.get();
      case INT_UNSIGNED:
        return Types.IntegerType.unsigned();
      case BIGINT:
        return Types.LongType.get();
      case BIGINT_UNSIGNED:
        return Types.LongType.unsigned();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Types.TimeType.get();
        // MySQL converts TIMESTAMP values from the current time zone to UTC for storage, and back
        // from UTC to the current time zone for retrieval. (This does not occur for other types
        // such as DATETIME.) see more details:
        // https://dev.mysql.com/doc/refman/8.0/en/datetime.html
      case TIMESTAMP:
        return Types.TimestampType.withTimeZone();
      case DATETIME:
        return Types.TimestampType.withoutTimeZone();
      case DECIMAL_UNSIGNED:
      case DECIMAL:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case VARCHAR:
        return Types.VarCharType.of(typeBean.getColumnSize());
      case CHAR:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case TEXT:
        return Types.StringType.get();
      case BINARY:
        return Types.BinaryType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      if (((Types.ByteType) type).signed()) {
        return TINYINT;
      } else {
        return TINYINT_UNSIGNED;
      }
    } else if (type instanceof Types.ShortType) {
      if (((Types.ShortType) type).signed()) {
        return SMALLINT;
      } else {
        return SMALLINT_UNSIGNED;
      }
    } else if (type instanceof Types.IntegerType) {
      if (((Types.IntegerType) type).signed()) {
        return INT;
      } else {
        return INT_UNSIGNED;
      }
    } else if (type instanceof Types.LongType) {
      if (((Types.LongType) type).signed()) {
        return BIGINT;
      } else {
        return BIGINT_UNSIGNED;
      }
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.StringType) {
      return TEXT;
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType) {
      // MySQL converts TIMESTAMP values from the current time zone to UTC for storage, and back
      // from UTC to the current time zone for retrieval. (This does not occur for other types
      // such as DATETIME.) see more details: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
      return ((Types.TimestampType) type).hasTimeZone() ? TIMESTAMP : DATETIME;
    } else if (type instanceof Types.DecimalType) {
      return type.simpleString();
    } else if (type instanceof Types.VarCharType) {
      return type.simpleString();
    } else if (type instanceof Types.FixedCharType) {
      return type.simpleString();
    } else if (type instanceof Types.BinaryType) {
      return type.simpleString();
    } else if (type instanceof Types.BooleanType) {
      return BIT;
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to MySQL type", type.simpleString()));
  }
}
