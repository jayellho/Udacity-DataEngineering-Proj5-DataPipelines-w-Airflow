from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,

                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = 'redshift',
                 test_sql_queries = None,
                 expected_results = None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.test_sql_queries = test_sql_queries
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        x = 0
        for test, result in zip(self.test_sql_queries, self.expected_results):
            x += 1
            self.log.info('Running test {}...'.format(x))
            records = redshift.get_records(test)
            actual_results = records[0][0]
            self.log.info("Test case {} passed? {}".format(x, result == actual_results))
            if not result == actual_results:
                raise ValueError("Test case {} failed. To investigate: Expected = {}, Actual {}".format(x, result, actual_results))
            
                
        self.log.info('DataQualityOperator not implemented yet')