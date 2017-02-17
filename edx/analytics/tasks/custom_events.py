# coding=utf-8
import re
import logging
import luigi
import luigi.task

from edx.analytics.tasks.database_imports import ImportMysqlToHiveTableTask
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.event_type_dist import EventTypeDistributionTask
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.mongo import CourseStructureHiveTable
from edx.analytics.tasks.pathutil import EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
import datetime

from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


class HiveTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, OverwriteOutputMixin):
    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to export logs for. '
        'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
        description='The start date to export logs for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to export logs for.  Ignored if `interval` is provided. '
        'Default is today, UTC.',
    )

    def __init__(self, *args, **kwargs):
        super(HiveTableDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class ImportAuthUserTask(ImportMysqlToHiveTableTask):
    """
    Импорт таблицы auth_user из базы Edx в Hive
    для возможности сопоставлять id и username студентов
    """
    @property
    def table_name(self):
        return 'auth_user'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
        ]


class ImportStudentEnrollmentTask(ImportMysqlToHiveTableTask):
    """
    Импорт таблицы student_courseenrollment из базы Edx в Hive
    для получения информации о том, на какой режим прохождения записан студент
    """

    @property
    def table_name(self):
        return 'student_courseenrollment'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('mode', 'STRING')
        ]


class CustomEventTypeDistributionTask(EventTypeDistributionTask):
    def init_local(self):
        super(EventTypeDistributionTask, self).init_local()

    def requires_local(self):
        return []

    def output(self):
        return get_target_from_url(self.output_root)

    def reducer(self, key, values):
        yield (key), (values)

    def get_event(self, line):
        event = eventlog.parse_json_event(line)
        if type(event) != dict:
            return None
        return event

    def get_event_date_str(self, event):
        event_time = self.get_event_time(event)
        if event_time is None:
            return None
        event_date_str = event_time.split("T")[0]
        if event_date_str < self.lower_bound_date_string or event_date_str >= self.upper_bound_date_string:
            return None
        return event_date_str

# --------------------------------------------------ACTIVITY------------------------------------------------------------


class ActivityDistributionTask(CustomEventTypeDistributionTask):
    """
    Получение событий по активности студентов из tracking.log
    В итоге получаем количество событий для ключа (user_id, event_date, org_id, course_id, event_type)
    """

    COURSE_NEWS_EVENT_TYPE = "news"

    known_events = [
        'play_video',
        'problem_check',
        'edx.forum.comment.created',
        'edx.forum.response.created',
        'edx.forum.response.voted',
        'edx.forum.thread.created',
        'edx.forum.thread.voted',
        'book',
        COURSE_NEWS_EVENT_TYPE
    ]

    COURSE_NEWS_PATTERN = "/courses/[^/]+/info"

    def mapper(self, line):
        event = self.get_event(line)
        if event is None:
            return
        event_date_str = self.get_event_date_str(event)
        if event_date_str is None:
            return
        event_type = event.get('event_type')
        if event_type.startswith('/'):
            if re.match(self.COURSE_NEWS_PATTERN, event_type):
                event_type = self.COURSE_NEWS_EVENT_TYPE
            else:
                return
        if event_type is None or event_type not in self.known_events:
            # Ignore if any of the keys is None
            return
        context = event.get('context', {})
        org_id = context.get('org_id')
        course_id = context.get('course_id')
        user_id = context.get('user_id')
        yield (user_id, event_date_str, org_id, course_id, event_type), 1

    def reducer(self, key, values):
        yield (key), sum(values)


class ActivityHiveTable(HiveTableDownstreamMixin, HiveTableTask):
    """
    Описание Hive таблицы для хранения данных об активности студентов
    """
    @property
    def table(self):
        return 'activity_log'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('event_date', 'STRING'),
            ('org_id', 'STRING'),
            ('course_id', 'STRING'),
            ('event_type', 'STRING'),
            ('event_count', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return ActivityDistributionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location
        )


class ActivityDistributionToSQLTaskWorkflow(HiveTableDownstreamMixin, HiveQueryToMysqlTask):
    """
    Базовый класс для выгрузки активности из Hive в базу отчетов
    """
    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            ActivityHiveTable(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path
            ),
            ImportAuthUserTask(),
            ImportStudentEnrollmentTask()
        )


class ActivityDaily(ActivityDistributionToSQLTaskWorkflow):
    """
    Выгрузка активности в базу отчетов.
    Данные из лога объединяются с данными о username и mode студентов из базы Edx
    """

    @property
    def query(self):
        query = """
                    SELECT
                        u.username,
                        a.event_date,
                        a.org_id,
                        a.course_id,
                        ce.mode,
                        a.event_type,
                        a.event_count
                    FROM activity_log a
                    INNER JOIN auth_user u ON u.id = a.user_id
                    INNER JOIN student_courseenrollment ce ON ce.user_id = u.id and ce.course_id = a.course_id
                """
        return query

    @property
    def table(self):
        return 'activity'

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255)'),
            ('event_date', 'DATE'),
            ('org_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('mode', 'VARCHAR(255)'),
            ('event_type', 'VARCHAR(255)'),
            ('event_count', 'INT'),
        ]

# -------------------------------------------------INVOLVEMENT----------------------------------------------------------


class InvolvementDaily(ActivityDistributionToSQLTaskWorkflow):
    """
    Выгрузка вовлеченности в базу отчетов.
    Отличается от активности только методом агрегирования,
    поэтому наследуется от базового класса выгрузки активности.
    Данные из лога объединяются с данными о username и mode студентов из базы Edx
    """

    @property
    def query(self):
        query = """
                    SELECT DISTINCT
                        u.username,
                        a.event_date,
                        a.org_id,
                        a.course_id,
                        ce.mode,
                        a.event_type
                    FROM activity_log a
                    INNER JOIN auth_user u ON u.id = a.user_id
                    INNER JOIN student_courseenrollment ce ON ce.user_id = u.id and ce.course_id = a.course_id
                """
        return query

    @property
    def table(self):
        return 'involvement'

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255)'),
            ('event_date', 'DATE'),
            ('org_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('mode', 'VARCHAR(255)'),
            ('event_type', 'VARCHAR(255)')
        ]
# ---------------------------------------------------ANSWER-------------------------------------------------------------


class AnswerDistributionTask(CustomEventTypeDistributionTask):
    """
    Получение данных об оценках студентов за обычные задания из tracking.log
    """

    known_events = [
        'problem_check',
    ]

    def mapper(self, line):
        event = self.get_event(line)
        if event is None:
            return
        event_date_str = self.get_event_date_str(event)
        if event_date_str is None:
            return
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        if event_type is None or event_type not in self.known_events \
                or event_source is None or event_source != 'server':
            # Ignore if any of the keys is None
            return

        event_event = event.get('event', {})
        grade = event_event.get('grade')
        max_grade = event_event.get('max_grade')
        problem_id = event_event.get('problem_id')
        problem_id_parts = re.split("[@/]", problem_id)
        if len(problem_id_parts) == 1:
            log.info("UNEXPECTED problem_id: {}".format(problem_id))
            raise Exception(problem_id)
        problem_id_last = problem_id_parts[-1]

        context = event.get('context', {})
        org_id = context.get('org_id')
        course_id = context.get('course_id')
        user_id = context.get('user_id')

        if event_type.startswith('/'):
            # Ignore events that begin with a slash
            return
        yield (user_id, event_date_str, org_id, course_id, grade, max_grade, problem_id, problem_id_last)

    reducer = NotImplemented



class AnswerHiveTable(HiveTableDownstreamMixin, HiveTableTask):
    """
    Описание Hive таблицы для хранения данных об оценках студентов
    """

    @property
    def table(self):
        return "answer_log"

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('event_date', 'STRING'),
            ('org_id', 'STRING'),
            ('course_id', 'STRING'),
            ('grade', 'INT'),
            ('max_grade', 'INT'),
            ('problem_id', 'STRING'),
            ('problem_id_last', 'STRING')
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return AnswerDistributionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location
        )


class AnswerDistributionToSQLTaskWorkflow(HiveTableDownstreamMixin, HiveQueryToMysqlTask):
    """
    Базовый класс для выгрузки данных об оценках студентов из Hive в базу отчетов
    """

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            AnswerHiveTable(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path
            ),
            ImportAuthUserTask(),
            ImportStudentEnrollmentTask(),
            CourseStructureHiveTable(warehouse_path=self.warehouse_path)
        )


class AnswerDaily(AnswerDistributionToSQLTaskWorkflow):
    """
    Выгрузка данных об оценках студентов из Hive в базу отчетов
    Данные из лога объединяются с данными о username и mode студентов из базы Edx,
    а также с данными о структуре курсов из базы Mongo.
    """

    @property
    def query(self):
        query = """
                    SELECT DISTINCT
                        u.username,
                        a.event_date,
                        a.org_id,
                        a.course_id,
                        ce.mode,
                        a.grade,
                        a.max_grade,
                        a.problem_id,
                        cs.block_id,
                        cs.block_name,
                        cs.format
                    FROM answer_log a
                    INNER JOIN auth_user u ON u.id = a.user_id
                    INNER JOIN student_courseenrollment ce ON ce.user_id = u.id and ce.course_id = a.course_id
                    INNER JOIN course_structure cs ON cs.child_id = a.problem_id_last
                """
        return query

    @property
    def table(self):
        return 'answer'

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255)'),
            ('event_date', 'DATE'),
            ('org_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('mode', 'VARCHAR(255)'),
            ('grade', 'INTEGER'),
            ('max_grade', 'INTEGER'),
            ('problem_id', 'VARCHAR(255)'),
            ('block_id', 'VARCHAR(255)'),
            ('block_name', 'VARCHAR(255)'),
            ('format', 'VARCHAR(255)')
        ]

# -------------------------------------------------- Open Assessment ---------------------------------------------------


class OpenAssessmentDistributionTask(CustomEventTypeDistributionTask):
    """
    Получение данных об оценках студентов за задания типа Open Response Assessment.
    Для справки по методике расчета см. раздел документации Edx:
    http://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs.html#open-response-assessment-events
    """

    known_events = [
        'openassessmentblock.peer_assess',
        'openassessmentblock.self_assess',
        'openassessmentblock.staff_assess',
    ]

    # оценка выставлена командой курса
    SCORE_TYPE_STAFF = "ST"
    # оценка выставлена другими студентами
    SCORE_TYPE_PEER = "PE"
    # оценка выставлена студентом самостоятельно
    SCORE_TYPE_SELF = "SE"

    def mapper(self, line):
        event = self.get_event(line)
        if event is None:
            return
        event_date_str = self.get_event_date_str(event)
        if event_date_str is None:
            return
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        if event_type is None or event_type not in self.known_events \
                or event_source is None or event_source != 'server':
            # Ignore if any of the keys is None
            return
        context = event.get('context', {})
        org_id = context.get('org_id')
        course_id = context.get('course_id')
        # id студента из auth_user
        user_id = context.get('user_id')

        module = context.get('module', {})
        # ключ вида: block-v1:hse+PRMN+spring_2016+type@openassessment+block@cc3d84bb182443c68dcdd1513c8297d7
        problem_id = module.get('usage_key', '')
        problem_id_parts = re.split("[@/]", problem_id)
        # id задания - последняя часть ключа: cc3d84bb182443c68dcdd1513c8297d7
        problem_id_last = problem_id_parts[-1]
        event_event = event.get('event', {})
        # тип оценки ST, PE или SE
        score_type = event_event.get("score_type")
        # анонимизированный id студента из student_anonymoususerid
        scorer_id = event_event.get("scorer_id")
        # уникальный id решения
        submission_uuid = event_event.get("submission_uuid")

        # разбиваем составную оценку на множество оценок по параметрам
        parts = event_event.get('parts', [])
        for part in parts:
            criterion = part['criterion']
            max_grade = criterion['points_possible']
            criterion_name = criterion['name']
            option = part['option']
            grade = option['points']
            yield (user_id, org_id, course_id, problem_id, problem_id_last, submission_uuid), \
                  (event_date_str, score_type, scorer_id, grade, max_grade, criterion_name)

    @staticmethod
    def median(lst):
        quotient, remainder = divmod(len(lst), 2)
        if remainder:
            return sorted(lst)[quotient]
        return sum(sorted(lst)[quotient - 1:quotient + 1]) / 2.

    def reducer(self, key, values):
        # в values содержатся все оценки по конкретному решению задания
        values = list(values)
        score_types = {val[1] for val in values}
        filtered_values = []
        # если среди оценок присутствуют оценки команды курса, берем в расчет только их
        if self.SCORE_TYPE_STAFF in score_types:
            staff_grades = filter(lambda v: v[1] == self.SCORE_TYPE_STAFF, values)
            staff_grades = sorted(staff_grades, key=lambda v: v[0], reverse=True)
            filtered_values = []
            latest_date = staff_grades[0][0]
            latest_scorer = staff_grades[0][2]
            # Оценок команды курса может быть много, но они перезаписывают друг друга.
            # Нам нужна последняя оценка по все параметрам
            for staff_grade in staff_grades:
                event_date = staff_grade[0]
                scorer_id = staff_grade[2]
                if event_date != latest_date or scorer_id != latest_scorer:
                    break
                filtered_values.append(staff_grade)
        # нет оценок команды курса, но есть оценки других студентов - берем в расчет только их
        elif self.SCORE_TYPE_PEER in score_types:
            # учитываем все оценки других студентов
            filtered_values = filter(lambda v: v[1] == self.SCORE_TYPE_PEER, values)
        # остаются только самостоятельные оценки - берем их
        elif self.SCORE_TYPE_SELF in score_types:
            self_grades = filter(lambda v: v[1] == self.SCORE_TYPE_SELF, values)
            self_grades = sorted(self_grades, key=lambda v: v[0], reverse=True)
            # Самостоятельных оценок может быть много, но они перезаписывают друг друга.
            # Нам нужна последняя оценка по все параметрам
            latest_date = self_grades[0][0]
            for self_grade in self_grades:
                event_date = self_grade[0]
                if event_date != latest_date:
                    break
                filtered_values.append(self_grade)
        else:  # не должно случиться
            raise Exception('Unknown score_types: {}'.format(str(score_types)))
        # получаем список оценок для каждого оцениваемого параметра
        # и суммарную максимально возможную оценку по всем параметрам
        criterion_grades = {}
        max_grade_sum = 0
        for v in filtered_values:
            grade = int(v[3])
            max_grade = int(v[4])
            criterion_name = v[5]
            if criterion_name not in criterion_grades:
                criterion_grades[criterion_name] = [grade]
                max_grade_sum += max_grade
            else:
                criterion_grades[criterion_name].append(grade)
        # находим медиану для каждого параметра и суммируем
        grade_sum = 0
        for criterion in criterion_grades.keys():
            grades = criterion_grades[criterion]
            grade_sum += self.median(grades)
        yield (key), (grade_sum, max_grade_sum)


class OpenAssessmentHiveTable(HiveTableDownstreamMixin, HiveTableTask):
    """
    Описание Hive таблицы для хранения данных об оценках студентов
    за задания типа Open Response Assessment.
    """
    @property
    def table(self):
        return "openassessment_log"

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('org_id', 'STRING'),
            ('course_id', 'STRING'),
            ('problem_id', 'STRING'),
            ('problem_id_last', 'STRING'),
            ('submission_uuid', 'STRING'),
            ('grade', 'DOUBLE'),
            ('max_grade', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return OpenAssessmentDistributionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location
        )


class OpenAssessmentToSQLTaskWorkflow(HiveTableDownstreamMixin, HiveQueryToMysqlTask):
    """
    Выгрузка оценок студентов за задания типа Open Response Assessment в базу отчетов.
    Данные из лога объединяются с данными о username и mode студентов из базы Edx,
    а также с данными о структуре курсов из базы Mongo.
    """

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            OpenAssessmentHiveTable(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path
            ),
            ImportAuthUserTask(),
            ImportStudentEnrollmentTask(),
            CourseStructureHiveTable(warehouse_path=self.warehouse_path)
        )

    @property
    def query(self):
        query = """
                    SELECT DISTINCT
                        u.username,
                        oa.org_id,
                        oa.course_id,
                        ce.mode,
                        oa.problem_id,
                        oa.submission_uuid,
                        oa.grade,
                        oa.max_grade,
                        cs.block_id,
                        cs.block_name,
                        cs.format
                    FROM openassessment_log oa
                    INNER JOIN auth_user u ON u.id = oa.user_id
                    INNER JOIN student_courseenrollment ce ON ce.user_id = u.id and ce.course_id = oa.course_id
                    INNER JOIN course_structure cs ON cs.child_id = oa.problem_id_last
                """
        return query

    @property
    def table(self):
        return 'openassessment'

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255)'),
            ('org_id', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('mode', 'VARCHAR(255)'),
            ('problem_id', 'VARCHAR(255)'),
            ('submission_uuid', 'VARCHAR(255)'),
            ('grade', 'DOUBLE'),
            ('max_grade', 'INTEGER'),
            ('block_id', 'VARCHAR(255)'),
            ('block_name', 'VARCHAR(255)'),
            ('format', 'VARCHAR(255)')
        ]


# ------------------------------------------------------------------------------------------------------

@workflow_entry_point
class ActivityWorkflow(
        HiveTableDownstreamMixin,
        luigi.WrapperTask):

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite
        }
        yield (
            ActivityDaily(**kwargs),
            InvolvementDaily(**kwargs),
            AnswerDaily(**kwargs),
            OpenAssessmentToSQLTaskWorkflow(**kwargs)
        )
