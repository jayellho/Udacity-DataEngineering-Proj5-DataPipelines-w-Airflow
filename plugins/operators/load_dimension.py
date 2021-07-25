from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {} ({}) {};
    """
    
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = 'redshift',
                 target_table = '',
                 insert_columns = '',
                 insert_sql = '',
                 truncate_table = False,                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.insert_columns = insert_columns
        self.insert_sql = insert_sql
        self.truncate_table = truncate_table
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info('Truncating Redshift table...')
            redshift.run(self.truncate_sql.format(self.target_table))
        
        self.log.info('Inserting Redshift table for dimensions table...')
        redshift.run(self.insert_sql.format(self.target_table, self.insert_columns, self.insert_sql))

        
