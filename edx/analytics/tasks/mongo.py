# coding=utf-8
import abc

import luigi
import logging
import datetime
from pymongo import MongoClient

from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.url import url_path_join, get_target_from_url
from edx.analytics.tasks.util.hive import HiveQueryToMysqlTask, HivePartition, WarehouseMixin, HiveTableTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

logger = logging.getLogger('luigi-interface')


class MongoImportTask(OverwriteOutputMixin, luigi.Task):
    """
    Базовый класс для импорта collection из MongoDB
    """
    output_root = luigi.Parameter(description='URL to store the data.')

    @property
    def collection_name(self):
        raise RuntimeError("Please specify collection name to import")

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'data.tsv'))

    def _get_collection(self):
        mongo_conn = luigi.configuration.get_config().get('mongodb', 'mongo_conn')
        mongo_db = luigi.configuration.get_config().get('mongodb', 'mongo_db')
        mc = MongoClient("{}/{}".format(mongo_conn, mongo_db))
        db = mc[mongo_db]
        return db[self.collection_name]

    @abc.abstractmethod
    def process_entry(self, entry):
        raise RuntimeError("Please implement the process_entry method")

    def run(self):
        self.remove_output_on_overwrite()
        col = self._get_collection()
        results = col.find({})
        with self.output().open('w') as output_file:
            for result in results:
                for values_list in self.process_entry(result):
                    output_file.write('\t'.join([unicode(v).encode('utf8') for v in values_list]) + '\n')


# ---------------------------------------------------------------------------------------------------------------------- 


class ActiveVersionsMongoImportTask(MongoImportTask):
    """
    Импорт коллекции modulestore.active_versions,
    хранящей данные об активных версиях структур курсов
    """

    @property
    def collection_name(self):
        return 'modulestore.active_versions'

    def process_entry(self, entry):
        uid = str(entry['_id'])
        run = entry['run']
        course = entry['course']
        org = entry['org']
        versions = entry.get('versions', {})
        if 'published-branch' not in versions:
            return []
        published_branch = str(versions['published-branch'])
        course_id = "course-v1:{}+{}+{}".format(org, course, run)
        return [(uid, published_branch, run, course, org, course_id)]


class ActiveVersionsHiveTable(HiveTableTask):
    """
    Описание Hive таблицы для хранения active_versions
    """
    @property
    def table(self):
        return 'active_versions'

    @property
    def columns(self):
        return [
            ('id', 'STRING'),
            ('published_branch', 'STRING'),
            ('run', 'STRING'),
            ('course', 'STRING'),
            ('org', 'STRING'),
            ('course_id', 'STRING')
        ]

    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    def requires(self):
        return ActiveVersionsMongoImportTask(
            output_root=self.partition_location
        )


# ----------------------------------------------------------------------------------------------------------------------


class DefinitionsGraderMongoImportTask(MongoImportTask):
    """
    Импорт данных о политике оценивания прохождения курса студентами
    из коллекции modulestore.definitions
    """

    @property
    def collection_name(self):
        return 'modulestore.definitions'

    def process_entry(self, entry):
        result = []
        uid = entry['_id']
        fields = entry['fields']
        grading_policy = fields.get('grading_policy', {})
        graders = grading_policy.get('GRADER', [])
        for grader_order, grader in enumerate(graders):
            min_count = grader.get("min_count")
            weight = grader.get("weight")
            type = grader.get("type")
            drop_count = grader.get("drop_count")
            short_label = grader.get("short_label")
            result.append((uid, type, short_label, min_count, drop_count, weight, grader_order))
        return result


class DefinitionsGraderHiveTable(HiveTableTask):
    """
    Описание Hive таблицы для хранения данных о политике оценивания
    прохождения курса студентами
    """
    @property
    def table(self):
        return 'definitions_grader'

    @property
    def columns(self):
        return [
            ('uid', 'STRING'),
            ('format', 'STRING'),
            ('abbr', 'STRING'),
            ('min_count', 'INT'),
            ('drop_count', 'INT'),
            ('weight', 'DOUBLE'),
            ('grader_order', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    def requires(self):
        return DefinitionsGraderMongoImportTask(
            output_root=self.partition_location
        )


class DefinitionsGraderToSQLTaskWorkflow(HiveQueryToMysqlTask):
    """
    Выгрузка данных о политике оценивания прохождения курса студентами в базу отчетов.
    Данные объединяются с active_versions и course_structure
    """
    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            ActiveVersionsHiveTable(warehouse_path=self.warehouse_path),
            CourseStructureHiveTable(warehouse_path=self.warehouse_path),
            DefinitionsGraderHiveTable(warehouse_path=self.warehouse_path)
        )

    @property
    def query(self):
        query = """SELECT dg.uid, course_def.course_id, dg.format, dg.abbr, dg.min_count, dg.drop_count, dg.weight, dg.grader_order
                   FROM (
                     SELECT DISTINCT v.course_id, cs.definition_id
                     FROM active_versions v
                     INNER JOIN course_structure cs ON v.published_branch = cs.branch_id
                   ) course_def
                   INNER JOIN definitions_grader dg on course_def.definition_id = dg.uid
                """
        return query

    @property
    def table(self):
        return 'grader'

    @property
    def columns(self):
        return [
            ('uid', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('format', 'VARCHAR(255)'),
            ('abbr', 'VARCHAR(255)'),
            ('min_count', 'INTEGER'),
            ('drop_count', 'INTEGER'),
            ('weight', 'DOUBLE'),
            ('grader_order', 'INTEGER'),
        ]


class DefinitionsCutoffsMongoImportTask(MongoImportTask):
    """
    Импорт данных о доступных оценках за курс и их значениях в %
    из коллекции modulestore.definitions
    """
    @property
    def collection_name(self):
        return 'modulestore.definitions'

    def process_entry(self, entry):
        result = []
        uid = entry['_id']
        fields = entry['fields']
        grading_policy = fields.get('grading_policy', {})
        cutoffs = grading_policy.get('GRADE_CUTOFFS', {})
        for name, percent in cutoffs.items():
            result.append((uid, name, percent))
        return result


class DefinitionsCutoffsHiveTable(HiveTableTask):
    """
    Описание Hive таблицы для хранения доступных оценок за курс
    """

    @property
    def table(self):
        return 'definitions_cutoff'

    @property
    def columns(self):
        return [
            ('uid', 'STRING'),
            ('name', 'STRING'),
            ('percent', 'DOUBLE'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    def requires(self):
        return DefinitionsCutoffsMongoImportTask(
            output_root=self.partition_location
        )


class DefinitionsCutoffToSQLTaskWorkflow(HiveQueryToMysqlTask):
    """
    Выгрузка доступных оценок за курс в базу отчетов.
    Данные объединяются с active_versions и course_structure
    """

    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            ActiveVersionsHiveTable(warehouse_path=self.warehouse_path),
            CourseStructureHiveTable(warehouse_path=self.warehouse_path),
            DefinitionsCutoffsHiveTable(warehouse_path=self.warehouse_path)
        )

    @property
    def query(self):
        query = """SELECT dc.uid, course_def.course_id, dc.name, dc.percent 
                       FROM (
                         SELECT DISTINCT v.course_id, cs.definition_id
                         FROM active_versions v
                         INNER JOIN course_structure cs ON v.published_branch = cs.branch_id
                       ) course_def
                       INNER JOIN definitions_cutoff dc on course_def.definition_id = dc.uid
                """
        return query

    @property
    def table(self):
        return 'cutoff'

    @property
    def columns(self):
        return [
            ('uid', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('name', 'VARCHAR(255)'),
            ('percent', 'DOUBLE'),
        ]


# ----------------------------------------------------------------------------------------------------------------------

class CourseStructureMongoImportTask(MongoImportTask):
    """
    Импорт структуры курсов из коллекции modulestore.structures
    """
    @property
    def collection_name(self):
        return 'modulestore.structures'

    def get_graded_parent(self, block_id, parent_dict):
        """
        Получение родительского блока с graded=True
        :param block_id: id дочернего блока
        :param parent_dict: словарь для поиска родителей
        :return: родительский блок с graded=True или None, если такого нет
        """
        parent = parent_dict.get(block_id)
        if parent is None:
            return None
        fields = parent['fields']
        graded = fields.get('graded')
        if graded:
            return parent
        parent_id = parent['block_id']
        return self.get_graded_parent(parent_id, parent_dict)

    def get_block_ordering(self, root, block_dict, order_dict, order=0):
        """
        Нумеруем все блоки по порядку их следования в дереве
        рекурсивно, начиная с ordered_dict[root]=order сверху вниз и слева направо
        :param root: корневой блок, с которого начинать нумерацию
        :param block_dict: словарь для получения блока по его id
        :param order_dict: словарь с пронумерованными блоками
        :param order: начальный номер
        :return: (ordered_dict, order), где order - следующий неиспользованный номер
        """
        fields = root['fields']
        children = fields.get('children', [])
        root_id = root['block_id']
        order_dict[root_id] = order
        order += 1
        for child_type_id in children:
            child_type, child_id = child_type_id
            child = block_dict[child_id]
            d, order = self.get_block_ordering(child, block_dict, order_dict, order)
            order_dict.update(d)
        return order_dict, order

    def process_entry(self, entry):
        result = []
        branch_id = str(entry["_id"])
        root_id = entry['root'][1]
        blocks = entry['blocks']
        # для более удобной навигации составляем словарь,
        # в котором ключ - child_id, а значение - parent блок
        # parent_dict[child_id] = parent
        # Кроме этого находим блок root по его id
        parent_dict = {}
        block_dict = {}
        definition_id = None
        root = None
        for block in blocks:
            block_id = block['block_id']
            block_dict[block_id] = block
            fields = block['fields']
            children = fields.get('children', [])
            if block["block_id"] == root_id:
                definition_id = str(block['definition'])
                root = block
            for child in children:
                child_type, child_id = child
                parent_dict[child_id] = block
        # нумеруем блоки начиная с root
        order_dict, max_order = self.get_block_ordering(root, block_dict, {})
        # нумеруем блоки вне основной иерархии
        for block in blocks:
            block_id = block['block_id']
            if block_id not in order_dict:
                order_dict, max_order = self.get_block_ordering(block, block_dict, order_dict, order=max_order)
        # Ищем блоки типа problem, у которых есть родитель с graded=True
        for block in blocks:
            block_type = block["block_type"]
            if block_type != "problem":
                continue
            block_id = block['block_id']
            parent = self.get_graded_parent(block_id, parent_dict)
            if parent is None:
                continue
            parent_id = parent['block_id']
            fields = parent['fields']
            display_name = fields.get('display_name')
            task_format = fields.get('format')
            weight = fields.get('weight')
            block_order = order_dict[block_id]
            result.append((block_id, parent_id, branch_id, display_name, task_format, weight, definition_id, block_order))
        return result


class CourseStructureHiveTable(HiveTableTask):
    """
    Описание Hive таблицы для хранения струтктуры курсов
    """
    @property
    def table(self):
        return 'course_structure'

    @property
    def columns(self):
        return [
            ('child_id', 'STRING'),
            ('block_id', 'STRING'),
            ('branch_id', 'STRING'),
            ('block_name', 'STRING'),
            ('format', 'STRING'),
            ('weight', 'DOUBLE'),
            ('definition_id', 'STRING'),
            ('block_order', 'INT'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    def requires(self):
        return CourseStructureMongoImportTask(
            output_root=self.partition_location
        )


class CourseStructureToSQLTaskWorkflow(HiveQueryToMysqlTask):
    """
    Выгрузка структуры курсов в базу отчетов.
    Данные объединяются с active_versions,
    чтобы выбрать только последнюю опубликованную версию структуры курса
    """
    @property
    def partition(self):
        return HivePartition('dt', datetime.datetime.today().date().isoformat())  # pylint: disable=no-member

    @property
    def required_table_tasks(self):
        yield (
            ActiveVersionsHiveTable(warehouse_path=self.warehouse_path),
            CourseStructureHiveTable(warehouse_path=self.warehouse_path)
        )

    @property
    def query(self):
        query = """SELECT DISTINCT cs.child_id, cs.block_id, cs.branch_id, cs.block_name, cs.format, cs.weight, 
                                   v.course_id, cs.block_order
                   FROM course_structure cs
                   INNER JOIN active_versions v ON v.published_branch = cs.branch_id
                """
        return query

    @property
    def table(self):
        return 'problem_groups'

    @property
    def columns(self):
        return [
            ('problem_id', 'VARCHAR(255)'),
            ('block_id', 'VARCHAR(255)'),
            ('branch_id', 'VARCHAR(255)'),
            ('block_name', 'VARCHAR(255)'),
            ('format', 'VARCHAR(255)'),
            ('weight', 'INTEGER'),
            ('course_id', 'VARCHAR(255)'),
            ('block_order', 'INTEGER'),
        ]


# ----------------------------------------------------------------------------------------------------------------------


@workflow_entry_point
class MongoImportTaskWorkflow(WarehouseMixin, OverwriteOutputMixin, luigi.WrapperTask):

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite
        }
        yield (
            CourseStructureToSQLTaskWorkflow(**kwargs),
            DefinitionsGraderToSQLTaskWorkflow(**kwargs),
            DefinitionsCutoffToSQLTaskWorkflow(**kwargs)
        )


