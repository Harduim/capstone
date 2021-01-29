from typing import Tuple

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    The operator's main functionality is to receive one or more SQL based test cases along with the
    expected results and execute the tests. For each the test, the test result and expected result
    needs to be checked and if there is no match, the operator should raise an exception and the
    task should retry and fail eventually.
    Results must be one row only for simplicity
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, conn_id: str, test_cases: Tuple[Tuple[str, callable]], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.test_cases = test_cases

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        for query, criteria in self.test_cases:
            result = hook.get_records(query)

            msg = f"\nResult:{result}\nQuery:{query}\n"
            assert criteria(result[0], context), "\n\nQuality test FAILED: " + msg
            self.log.info("Quality test above sucessfull." + msg)
