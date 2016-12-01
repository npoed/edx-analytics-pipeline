"""
Support for running a SQL script against an HP Vertica database.
"""
from collections import namedtuple
import json
import datetime
import logging

import luigi
import luigi.configuration
from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.util.vertica_target import VerticaTarget, CredentialFileVerticaTarget

log = logging.getLogger(__name__)

try:
    import vertica_python
    vertica_client_available = True  # pylint: disable-msg=C0103
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable-msg=C0103


class RunSqlScriptTaskMixin(object):
    """
    Parameters for running a SQL script against an HP Vertica database.

    """
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )
    schema = luigi.Parameter(
        config_path={'section': 'run-sql-script', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'run-sql-script', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    read_timeout = luigi.IntParameter(
        config_path={'section': 'run-sql-script', 'name': 'read_timeout'}
    )
    source_script = luigi.Parameter(
        description='The path to the source script to execute'
    )
    table = luigi.Parameter(
        description='The table which this script creates as a result, or more generally, '
        'a unique identifier for the purposes of tracking whether or not this script ran '
        'successfully'
    )
    raise_on_error = luigi.BooleanParameter(
        default=False,
        description='Whether or not we raise any exceptions that occur during execution. '
        'Not all tasks necessarily care about running to completion, and we might not want '
        'to halt the parent requiring us if we fail.'
    )
    marker_schema = luigi.Parameter(
        default=None,
        description='The marker schema to which to write the marker table. marker_schema would '
        'default to the schema value if the value here is None.'
    )


class RunSqlScriptTask(RunSqlScriptTaskMixin, luigi.Task):
    """
    A task for running a SQL script against an HP Vertica database.
    """
    required_tasks = None
    output_target = None

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'source_script': ExternalURL(url=self.source_script),
            }
        return self.required_tasks

    def update_id(self):
        """This update id will be a unique identifier for this SQL script execution."""
        return str(self)

    def output(self):
        """
        Returns a VerticaTarget representing the inserted dataset.
        """
        if self.output_target is None:
            self.output_target = CredentialFileVerticaTarget(
                credentials_target=self.input()['credentials'],
                table=self.table,
                schema=self.schema,
                update_id=self.update_id(),
                read_timeout=self.read_timeout,
                marker_schema=self.marker_schema,
            )

        return self.output_target

    def run(self):
        """
        Runs the given SQL script against the Vertica target.
        """
        if not self.table:
            raise Exception("table needs to be specified")

        # Make sure we can connect to Vertica.
        self.check_vertica_availability()
        connection = self.output().connect()

        try:
            # Set up our connection to point to the specified schema so that scripts can have unqualified
            # table references and not necessarily need to know or care about where they're running.
            connection.cursor().execute('SET SEARCH_PATH = {schema};'.format(schema=self.schema))

            with self.input()['source_script'].open('r') as script_file:
                # Read in our script and execute it.
                script_body = script_file.read()
                connection.cursor().execute(script_body)

                # If we're here, nothing blew up, so mark as complete.
                self.output().touch(connection)

                connection.commit()
                log.debug("Committed transaction.")
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            if self.raise_on_error:
              raise
        finally:
            connection.close()

    def check_vertica_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Vertica client library available."""
        if not vertica_client_available:
            raise ImportError('Vertica client library not available')
