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

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{window, _}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{window, _}
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging

object NetworkWordCount {


  def main(args: Array[String]) {

    lazy val spark = SparkSession.builder
      .master("local[*]")
      .appName("Apple spark examples")
      //.enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.types._
    import spark.implicits._
    val postSchema: StructType = new StructType()
      .add("commentCount", IntegerType, true)
      .add("lastActivityDate", TimestampType, true)
      .add("ownerUserId", StringType, true)
      .add("body", StringType, true)
      .add("score", IntegerType, true)
      .add("creationDate", TimestampType, true)
      .add("viewCount", StringType, true)
      .add("title", StringType, true)
      .add("tags", StringType, true)
      .add("answerCount", StringType, true)
      .add("acceptedAnswerId", StringType, true)
      .add("postTypeId", StringType, true)
      .add("id", LongType, false)


    val itPostsSplit = spark
      .read
      .option("delimiter", "~")
      .schema(postSchema)
      .csv("italianPosts.csv")

    val posts =
      itPostsSplit
        //.filter('postTypeId === 1)
        //.select(avg("score"))
        .withColumn("acceptedAnswerId", $"acceptedAnswerId".cast(LongType))
        .withColumn("ownerUserId", $"ownerUserId".cast(LongType))
        .withColumn("postTypeId", $"postTypeId".cast(LongType))
        .withColumn("answerCount", $"answerCount".cast(LongType))
        .withColumn("maxDateDiff", datediff('lastActivityDate, 'creationDate))


    val itVotesRaw = spark.sparkContext.textFile("italianVotes.csv").
      map(x => x.split("~"))
    val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong,
      row(2).toInt, Timestamp.valueOf(row(3))))
    val votesSchema = StructType(Seq(
      StructField("id", LongType, false),
      StructField("postId", LongType, false),
      StructField("voteTypeId", IntegerType, false),
      StructField("creationDate", TimestampType, false)))
    val votesDf = spark.createDataFrame(itVotesRows, votesSchema)

    posts
      .join(votesDf, posts("id") === votesDf("postId"), "outer")
    //  .show()

    val playerMakeSchema = new StructType()
      .add("id", IntegerType, false)
      .add("Name", StringType, false)

    val playerModelSchema = new StructType()
      .add("id", IntegerType, false)
      .add("Name", StringType, false)
      .add("makeId", IntegerType, true)

    val playerMakeDF = spark.read.schema(playerMakeSchema).csv("PlayerMake.csv")
    val playerModelDF = spark.read.schema(playerModelSchema).csv("PlayerModel.csv")

    //playerMakeDF.show()
    //playerModelDF.show()

//    playerModelDF
//      .join(playerMakeDF, playerMakeDF("id") === 'makeId,"leftouter")
//      .show()
//    playerModelDF
//      .join(playerMakeDF, playerMakeDF("id") === 'makeId,"left")
//      .show()
    playerModelDF
      .join(playerMakeDF, playerMakeDF("id") === 'makeId,"right ")
      .show()
    Console.readBoolean()

  }
}

// scalastyle:on println