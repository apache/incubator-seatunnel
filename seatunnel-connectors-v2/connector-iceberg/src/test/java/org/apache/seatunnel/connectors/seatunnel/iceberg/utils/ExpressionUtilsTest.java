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

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

public class ExpressionUtilsTest {

    @Test
    public void testSqlToExpression() throws JSQLParserException {
        String sql = "delete from test.a where id = 1";

        Expression expression = ExpressionUtils.convertDeleteSQL(sql);
        Assertions.assertEquals(Expressions.equal("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id != 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id > 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.greaterThan("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id >= 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.greaterThanOrEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id < 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.lessThan("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id <= 1";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.lessThanOrEqual("id", 1).toString(), expression.toString());

        sql = "delete from test.a where id is null";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.isNull("id").toString(), expression.toString());

        sql = "delete from test.a where id is not null";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notNull("id").toString(), expression.toString());

        sql = "delete from test.a where id in (1,2,3)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.in("id", 1, 2, 3).toString(), expression.toString());

        sql = "delete from test.a where id not in (1,2,3)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.notIn("id", 1, 2, 3).toString(), expression.toString());

        sql = "delete from test.a where id is true";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.equal("id", true).toString(), expression.toString());

        sql = "delete from test.a where id = 1 and name = a or (age >=1 and age < 1)";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(
                Expressions.or(
                                Expressions.and(
                                        Expressions.equal("id", 1), Expressions.equal("name", "a")),
                                Expressions.and(
                                        Expressions.greaterThanOrEqual("age", 1),
                                        Expressions.lessThan("age", 1)))
                        .toString(),
                expression.toString());

        sql = "delete from test.a where id = 'a'";
        expression = ExpressionUtils.convertDeleteSQL(sql);

        Assertions.assertEquals(Expressions.equal("id", "a").toString(), expression.toString());

        sql =
                "delete from test.a where f1 = '2024-01-01' and f2 = '12:00:00.001' and f3 = '2024-01-01 12:00:00.001'";
        Statement statement = CCJSqlParserUtil.parse(sql);
        Delete delete = (Delete) statement;
        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "f1", Types.DateType.get()),
                        Types.NestedField.optional(2, "f2", Types.TimeType.get()),
                        Types.NestedField.optional(3, "f3", Types.TimestampType.withoutZone()));
        expression = ExpressionUtils.convert(delete.getWhere(), schema);

        Assertions.assertEquals(
                Expressions.and(
                                Expressions.equal("f1", 19723),
                                Expressions.equal("f2", 43200001000L),
                                Expressions.equal("f3", 1704110400001000L))
                        .toString(),
                expression.toString());
    }
}
