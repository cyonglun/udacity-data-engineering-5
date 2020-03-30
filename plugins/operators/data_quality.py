from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_statements=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_statements = check_statements

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for statement in self.check_statements:
            self.log.info(f"Executing DataQualityOperator: {statement['sql']}")
            
            count = int(redshift_hook.get_records(statement['sql'])[0][0])

            if statement['operator'] == 'eq':
                if count != statement['value']:
                    raise ValueError(f"Data quality check failed for statement {count} == {statement['value']}")

            elif statement['operator'] == 'gt':
                if count <= statement['value']:
                    raise ValueError(f"Data quality check failed for statement {count} > {statement['value']}")

            elif statement['operator'] == 'lt':
                if count >= statement['value']:
                    raise ValueError(f"Data quality check failed for statement {count} < {statement['value']}")

            elif statement['operator'] == 'ne':
                if count == statement['value']:
                    raise ValueError(f"Data quality check failed for for statement {count} != {statement['value']}")

            self.log.info(f"Data quality check passed with {count} records")
        