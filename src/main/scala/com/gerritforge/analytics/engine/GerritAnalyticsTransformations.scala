// Copyright (C) 2017 GerritForge Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.analytics.engine

import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId, ZoneOffset, ZonedDateTime}

import com.gerritforge.analytics.model.{GerritEndpointConfig, ProjectContributionSource}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.util.Try

object GerritAnalyticsTransformations {

  implicit class PimpedRDDString(val rdd: RDD[String]) extends AnyVal {

    def enrichWithSource(implicit config: GerritEndpointConfig): RDD[ProjectContributionSource] = {
      rdd.map { projectName =>
        ProjectContributionSource(projectName, config.contributorsUrl(projectName))
      }
    }
  }

  def getLinesFromURL(sourceURL: String): Iterator[String] = {
    val is = new URL(sourceURL).openConnection.getInputStream
    new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
      .lines.iterator().asScala
      .filterNot(_.trim.isEmpty)
  }

  def getProjectJsonContributorsArray(project: String, sourceURL: String): Array[(String, String)] = {
    try {
      getLinesFromURL(sourceURL)
        .map(s => (project, s))
        .toArray
    } catch {
      case e: IOException => Array()
    }
  }

  def getEmailAliasDF(emailAliasesRDD: RDD[String])(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._
    Try {
      val df = spark.sqlContext.read.json(emailAliasesRDD).toDF()
      df.withColumn("email_alias", explode(df("emails"))).drop("emails")
    }.getOrElse(spark.emptyDataset[CommitterInfo].toDF())
  }

  case class CommitterInfo(author: String, email_alias: String)

  case class CommitInfo(sha1: String, date: Long, isMerge: Boolean)

  case class UserActivitySummary(year: Integer,
                                 month: Integer,
                                 day: Integer,
                                 hour: Integer,
                                 name: String,
                                 email: String,
                                 num_commits: Integer,
                                 num_files: Integer,
                                 added_lines: Integer,
                                 deleted_lines: Integer,
                                 commits: Array[CommitInfo],
                                 last_commit_date: Long)

  import org.apache.spark.sql.Encoders

  val schema = Encoders.product[UserActivitySummary].schema

  implicit class PimpedDataFrame(val df: DataFrame) extends AnyVal {
    def transformCommitterInfo()(implicit spark: SparkSession): DataFrame = {
      import org.apache.spark.sql.functions.from_json
      import spark.sqlContext.implicits._
      df.withColumn("json", from_json($"json", schema))
        .selectExpr(
          "project", "json.name as name", "json.email as email",
          "json.year as year", "json.month as month", "json.day as day", "json.hour as hour",
          "json.num_files as num_files", "json.added_lines as added_lines", "json.deleted_lines as deleted_lines",
          "json.num_commits as num_commits", "json.last_commit_date as last_commit_date")
    }

    def handleAuthorEMailAliases(emailAliasesDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
      import spark.sqlContext.implicits._
      df.join(emailAliasesDF, df("email") === emailAliasesDF("email_alias"), "left_outer" )
        .withColumn("author", coalesce($"author", $"name")).drop("name", "email_alias")
    }

    def convertDates(columnName: String)(implicit spark: SparkSession): DataFrame = {
      df.withColumn(columnName, longDateToISOUdf(col(columnName)))
    }

    def addOrganization()(implicit spark: SparkSession): DataFrame = {
      df.withColumn("organization", emailToDomainUdf(col("email")))
    }
  }

  private def emailToDomain(email: String): String = {
    email split "@" match {
      case parts if (parts.length == 2) => parts.last.toLowerCase
      case _ => ""
    }
  }

  private def emailToDomainUdf = udf(emailToDomain(_: String))


  implicit class PimpedRDDProjectContributionSource(val projectsAndUrls: RDD[ProjectContributionSource]) extends AnyVal {

    def fetchRawContributors(implicit spark: SparkSession): RDD[(String, String)] = {
      projectsAndUrls.flatMap {
        p => getProjectJsonContributorsArray(p.name, p.contributorsUrl)
      }
    }
  }

  import org.apache.spark.sql.functions.udf

  val longDateToISOUdf = udf(longDateToISO(_: Number))

  def longDateToISO(in: Number): String =
    ZonedDateTime.ofInstant(
      LocalDateTime.ofEpochSecond(in.longValue() / 1000L, 0, ZoneOffset.UTC),
      ZoneOffset.UTC, ZoneId.of("Z")
    ) format DateTimeFormatter.ISO_OFFSET_DATE_TIME

}