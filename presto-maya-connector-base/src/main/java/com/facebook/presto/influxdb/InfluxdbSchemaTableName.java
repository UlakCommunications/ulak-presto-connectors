//package com.facebook.presto.influxdb;
//
///*
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//import com.fasterxml.jackson.annotation.JsonCreator;
//import com.fasterxml.jackson.annotation.JsonProperty;
//
//import static io.airlift.slice.SizeOf.instanceSize;
//
//public class InfluxdbSchemaTableName extends io.trino.spi.connector.SchemaTableName
//{
//    private static final int INSTANCE_SIZE = instanceSize(InfluxdbSchemaTableName.class);
//
//    @JsonCreator
//    public InfluxdbSchemaTableName(@JsonProperty("schema") String schemaName, @JsonProperty("table") String tableName)
//    {
//        super(schemaName, tableName);
//        this.schemaName = schemaName;
//        this.tableName = tableName;
//    }
//
//    public static io.trino.spi.connector.SchemaTableName schemaTableName(String schemaName, String tableName)
//    {
//        return new  InfluxdbSchemaTableName(schemaName, tableName);
//    }
//}
