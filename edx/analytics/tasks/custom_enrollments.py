# coding=utf-8
"""Compute metrics related to user enrollments in courses"""

import logging
import datetime

import luigi.task

from edx.analytics.tasks.custom_mixins import OverwriteWorkflowMixin
from edx.analytics.tasks.edx_mysql_import import ImportAuthUserTask
from edx.analytics.tasks.enrollments import CourseEnrollmentTask, DaysEnrolledForEvents, \
    CourseEnrollmentTableDownstreamMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import HiveTableTask, HivePartition, HiveQueryToMysqlTask
from edx.analytics.tasks.decorators import workflow_entry_point

log = logging.getLogger(__name__)


class MyCourseEnrollmentTask(CourseEnrollmentTask):
    """
    Импорт данных о зачисленных на курсы студентах по дням из tracking.log
    """

    def mapper(self, line):
        result = super(MyCourseEnrollmentTask, self).mapper(line)
        if not result:
            return
        event = eventlog.parse_json_event(line) or {}
        context = event.get('context', {})
        org_id = context.get('org_id')
        for key, value in result:
            yield (org_id, ) + key, value

    def reducer(self, key, values):
        """Emit records for each day the user was enrolled in the course."""
        org_id, course_id, user_id = key

        event_stream_processor = DaysEnrolledForEvents(course_id, user_id, self.interval, values)
        for day_enrolled_record in event_stream_processor.days_enrolled():
            datestamp, course_id, user_id, enrolled_at_end, change_since_last_day, mode_at_end = day_enrolled_record
            if enrolled_at_end == DaysEnrolledForEvents.ENROLLED:
                yield (org_id, datestamp, course_id, user_id, enrolled_at_end, change_since_last_day, mode_at_end)


class CourseEnrollmentTableTask(CourseEnrollmentTableDownstreamMixin, HiveTableTask):
    """
    Описание Hive таблицы для хранения студентов, зачисленных на курсы по дням
    """

    @property
    def table(self):
        return 'course_enrollment'

    @property
    def columns(self):
        return [
            ('org_id', 'STRING'),
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('at_end', 'TINYINT'),
            ('change', 'TINYINT'),
            ('mode', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return MyCourseEnrollmentTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class EnrollmentTask(OverwriteWorkflowMixin, CourseEnrollmentTableDownstreamMixin, HiveQueryToMysqlTask):
    """Base class for breakdowns of enrollments"""

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            CourseEnrollmentTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                overwrite=self.hive_overwrite
            ),
            ImportAuthUserTask(overwrite=self.hive_overwrite)
        )

    @property
    def query_date(self):
        """We want to store demographics breakdown from the enrollment numbers of most recent day only."""
        query_date = self.interval.date_b - datetime.timedelta(days=1)
        return query_date.isoformat()


class EnrollmentDailyTask(EnrollmentTask):
    """
    История зачисленных студентов на каждом курсе по дням.
    """

    @property
    def query(self):
        query = """
            SELECT
                u.username,
                ce.org_id,
                ce.course_id,
                ce.date,
                ce.mode
            FROM course_enrollment ce
            INNER JOIN auth_user u ON u.id = ce.user_id
            WHERE ce.at_end = 1
        """.format(date=self.query_date)
        return query

    @property
    def table(self):
        return 'course_enrollment_daily'

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255) NOT NULL'),
            ('org_id', 'VARCHAR(255) NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('mode', 'VARCHAR(255) NOT NULL'),
        ]


@workflow_entry_point
class CustomEnrollmentTaskWorkflow(OverwriteWorkflowMixin, CourseEnrollmentTableDownstreamMixin, luigi.WrapperTask):
    """
    Выгрузка истории зачислений студентов на курсы по дням в базу отчетов
    """

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
            'hive_overwrite': self.hive_overwrite,
            'allow_empty_insert': self.allow_empty_insert,
        }
        yield (
            EnrollmentDailyTask(**kwargs),
        )
