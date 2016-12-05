"""
Support for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.
"""
from collections import namedtuple
import yaml
import datetime
import logging
from os import path

import luigi
import luigi.configuration
from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.run_sql_script import RunSqlScriptTask
from edx.analytics.tasks.util.sentinel_target import SentinelTarget

log = logging.getLogger(__name__)

class RunSqlScriptsTaskMixin(object):
    """
    Parameters for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.
    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    schema = luigi.Parameter(
        config_path={'section': 'run-sql-scripts', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'run-sql-scripts', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    read_timeout = luigi.IntParameter(
        config_path={'section': 'run-sql-scripts', 'name': 'read_timeout'}
    )
    script_configuration = luigi.Parameter(
        description='The path to the configuration file that specifies which scripts to run'
    )
    script_root = luigi.Parameter(
        default='',
        description='The root directory from which the script locations in the configuration '
        'are referenced from.'
    )
    marker_schema = luigi.Parameter(
        default=None,
        description='The marker schema to which to write the marker table. marker_schema would '
        'default to the schema value if the value here is None.'
    )


class RunSqlScriptsTask(RunSqlScriptsTaskMixin, luigi.Task):
    """
    A task for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.
    """
    required_tasks = None
    output_target = None

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'script_configuration': ExternalURL(url=self.script_configuration),
            }
        return self.required_tasks

    def update_id(self):
        """
        A unique string identifying this task run, based on the input parameters.
        """
        return str(self)

    def output(self):
        """
        Returns a SentinelTarget which represents a task that generates no output
        and also is not intended to be marked for complete.
        """
        if self.output_target is None:
            self.output_target = SentinelTarget()

        return self.output_target

    def run(self):
        """
        Executes the specified SQL scripts.
        """
        log.info("Running...")
        with self.input()['script_configuration'].open('r') as script_conf_file:
            # Read in our script configuration.
            config = yaml.safe_load(script_conf_file)
            for script in config['scripts']:
                log.info("Triggering task run for '{script}'...".format(script=script['location']))

                # Fire off a task for each script. Make sure raise_on_error is false because
                # we shouldn't stop the job if one subtask happens to fail.  This is hacky, because
                # the version of Luigi we're on is so old that it doesn't support the yield-tasks-from-run
                # functionality.  Alas.
                current_task = RunSqlScriptTask(
                    credentials=self.credentials, schema=self.schema, marker_schema=self.marker_schema,
                    date=self.date, read_timeout=self.read_timeout, source_script=path.join(self.script_root, script['location']),
                    table=script['table'], raise_on_error=False)
                luigi.build([current_task])
