# coding=utf-8
from edx.analytics.tasks.database_imports import ImportMysqlToHiveTableTask


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
