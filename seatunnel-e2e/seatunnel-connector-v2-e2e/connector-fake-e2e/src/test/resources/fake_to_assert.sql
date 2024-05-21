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

/* config
env {
  parallelism = 1
  job.mode = "BATCH"

  # You can set spark configuration here
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
}
*/

CREATE TABLE fake WITH (
   'connector'='FakeSource',
   'type' = 'source',
   'schema' = '{
      fields {
        c_map = "map<string, string>",
        c_array = "array<int>",
        c_string = string,
        c_boolean = boolean,
        c_tinyint = tinyint,
        c_smallint = smallint,
        c_int = int,
        c_bigint = bigint,
        c_float = float,
        c_double = double,
        c_bytes = bytes,
        c_date = date,
        c_decimal = "decimal(38, 18)",
        c_timestamp = timestamp,
        c_row = {
          c_map = "map<string, string>",
          c_array = "array<int>",
          c_string = string,
          c_boolean = boolean,
          c_tinyint = tinyint,
          c_smallint = smallint,
          c_int = int,
          c_bigint = bigint,
          c_float = float,
          c_double = double,
          c_bytes = bytes,
          c_date = date,
          c_decimal = "decimal(38, 18)",
          c_timestamp = timestamp
        }
      }
    }'
);

CREATE TABLE assert WITH (
  'connector' = 'Assert',
  'type' = 'sink',
  'rules' = '{
      row_rules = [
        {
          rule_type = MAX_ROW,
          rule_value = 5
        }
      ],
      field_rules = [
        {
          field_name = c_string,
          field_type = string,
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        },
        {
          field_name = c_boolean,
          field_type = boolean,
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        },
        {
          field_name = c_double,
          field_type = double,
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        }
      ]
    }'
);

INSERT INTO assert SELECT * FROM fake;