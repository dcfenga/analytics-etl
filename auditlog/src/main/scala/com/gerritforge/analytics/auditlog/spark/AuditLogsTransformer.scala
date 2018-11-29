// Copyright (C) 2018 GerritForge Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.analytics.auditlog.spark

import com.gerritforge.analytics.auditlog.broadcast.GerritUserIdentifiers
import com.gerritforge.analytics.auditlog.model.AuditEvent
import com.gerritforge.analytics.auditlog.model.ElasticSearchFields._
import com.gerritforge.analytics.auditlog.range.TimeRange
import com.gerritforge.analytics.auditlog.spark.dataframe.ops.DataFrameOps._
import com.gerritforge.analytics.auditlog.spark.rdd.ops.SparkRDDOps._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AuditLogsTransformer(gerritIdentifiers: GerritUserIdentifiers = GerritUserIdentifiers.empty)(implicit spark: SparkSession) {

  private val broadcastUserIdentifiers = spark.sparkContext.broadcast(gerritIdentifiers)

  def transform(auditEventsRDD: RDD[AuditEvent], timeAggregation: String, timeRange: TimeRange = TimeRange.always): DataFrame =
    auditEventsRDD
      .filterWithinRange(TimeRange(timeRange.since, timeRange.until))
      .toJsonString
      .toJsonTableDataFrame
      .hydrateWithUserIdentifierColumn(USER_IDENTIFIER_FIELD, broadcastUserIdentifiers.value)
      .withTimeBucketColum(TIME_BUCKET_FIELD, timeAggregation)
      .withCommandColumns(COMMAND_FIELD, COMMAND_ARGS_FIELD)
      .aggregateNumEventsColumn(NUM_EVENTS_FIELD, FACETING_FIELDS)
}
