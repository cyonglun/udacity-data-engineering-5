from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#8EB6D4'

    copy_sql = """
        COPY {table}
        FROM 's3://{s3_bucket}/{s3_key}'
        with credentials
        'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
        {copy_options};
    """
    
    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 table,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 copy_options='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info(f'Executing StageToRedshiftOperator from {self.s3_bucket}/{self.s3_key} to {self.table} table')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
     
        self.log.info('Executing COPY statement')
        formatted_copy_sql = StageToRedshiftOperator.copy_sql.format(
            table = self.table,
            s3_bucket = self.s3_bucket,
            s3_key = self.s3_key,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            copy_options = self.copy_options
        )
        
        redshift_hook.run(formatted_copy_sql)
        
        self.log.info("StageToRedshiftOperator completed successfully")