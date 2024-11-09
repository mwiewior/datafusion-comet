/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.exec

import org.biodatageeks.sequila.rangejoins.IntervalTree.IntervalTreeJoinStrategyOptim

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import org.apache.comet.CometConf.COMET_EXEC_CONFIG_PREFIX

class SeQuiLaSuite extends CometTestBase {
  val schema1: StructType = StructType(
    Seq(StructField("start1", IntegerType, nullable = false), StructField("end1", IntegerType)))
  val schema2: StructType = StructType(
    Seq(StructField("start2", IntegerType, nullable = false), StructField("end2", IntegerType)))
  val schema3: StructType = StructType(
    Seq(
      StructField("chr1", StringType, nullable = false),
      StructField("start1", IntegerType, nullable = false),
      StructField("end1", IntegerType, nullable = false)))

  def prepareData(): Unit = {
    val partNum = 8
    val chainRn4 = "/Users/mwiewior/research/data/AIListTestData/sequila-native/chainRn4.bed"
    val chainVicPac2 =
      "/Users/mwiewior/research/data/AIListTestData/sequila-native/chainVicPac2.bed"
    spark.read
      .options(Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> "\t"))
      .csv(chainRn4)
      .repartition(partNum)
      .write
      .parquet("/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4.parquet")
    spark.read
      .options(Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> "\t"))
      .csv(chainVicPac2)
      .repartition(partNum)
      .write
      .parquet("/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2.parquet")

  }

  test("Prepare data") {
    prepareData()
  }

  test("SeQuiLaAnalyzer") {

    val ds3 = spark.read.parquet(
      "/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainRn4.parquet/*.parquet")
    val ds4 = spark.read.parquet(
      "/Users/mwiewior/CLionProjects/sequila-native/sandbox/chainVicPac2.parquet/*.parquet")

    ds3.createOrReplaceTempView("chainRn4")
    ds4.createOrReplaceTempView("chainVicPac2")

    spark.experimental.extraStrategies = new IntervalTreeJoinStrategyOptim(spark) :: Nil

    spark.conf.set(s"$COMET_EXEC_CONFIG_PREFIX.interval_join.enabled", "true")
    val sqlQuery =
      "select count(*) from chainVicPac2 a, chainRn4 b  where (a.column0=b.column0 and a.column2>=b.column1 and a.column1<=b.column2);"
    spark.time {
      spark
        .sql(sqlQuery)
        .show()
    }
//    val sqlQuery =
//      "select s3.chr1 as s3_chr,s3.start1 as s3_start1, s3.*,s4.* from s4 JOIN s3 WHERE s3.chr1=s4.chr1 and s3.end1>=s4.start1 and s3.start1<=s4.end1"
    spark.sql(sqlQuery).explain(true)
//    assert(spark.sql(sqlQuery).count() == 16)

  }
}
