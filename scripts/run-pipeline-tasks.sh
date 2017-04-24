#!/bin/bash

. /edx/app/hadoop/hadoop-2.3.0/hadoop_env

PREV_DATE=`date --date="1 days ago" +"%Y-%m-%d"`
CUR_DATE=`date +"%Y-%m-%d"`

cd /edx/app/pipeline/edx-analytics-pipeline

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/edx/app/hadoop/hadoop/bin:/edx/app/hadoop/hadoop/sbin:/edx/bin:/edx/app/hadoop/hive/bin:/edx/app/hadoop/sqoop/bin:$PATH

/edx/app/hadoop/hadoop/bin/hadoop fs -rm -r hdfs://127.0.0.1:9000/edx-analytics-pipeline/warehouse/*
hive -hiveconf PREV_DATE=$PREV_DATE -f /edx/app/pipeline/edx-analytics-pipeline/scripts/clear.hql
hive -hiveconf PREV_DATE=$CUR_DATE -f /edx/app/pipeline/edx-analytics-pipeline/scripts/clear.hql

/edx/app/pipeline/pipeline/bin/launch-task ActivityWorkflow --local-scheduler --n-reduce-tasks 4 --interval $PREV_DATE-$CUR_DATE --overwrite --allow-empty-insert
/edx/app/pipeline/pipeline/bin/launch-task CustomEnrollmentTaskWorkflow --local-scheduler --n-reduce-tasks 4 --interval $PREV_DATE-$CUR_DATE --overwrite --allow-empty-insert
/edx/app/pipeline/pipeline/bin/launch-task MongoImportTaskWorkflow --local-scheduler --overwrite --allow-empty-insert
