from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 table="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.table = table
        self.mode = mode

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator on: {self.sql_statement}')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.mode == "truncate":
            self.log.info(f"Truncating {self.table}")
            redshift_hook.run(f"DELETE FROM {self.table};")
            
            
        redshift_hook.run(self.sql_statement)
        