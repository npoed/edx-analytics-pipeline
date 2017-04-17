# coding=utf-8
import luigi
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin


class OverwriteWorkflowMixin(OverwriteOutputMixin):
    hive_overwrite = luigi.BooleanParameter(
        default=False,
        description='If True, overwrite the hive data.',
    )

    allow_empty_insert = luigi.BooleanParameter(
        default=False,
        description='Whether or not to allow overwrite outputs with empty result set; set to False by default for now.',
        significant=False
    )
