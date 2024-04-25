import os
from .mysql_io_manager import MySQLIOManager
from .minio_io_manager import MinIOIOManager
from .psql_io_manager import PostgreSQLIOManager


mysql = MySQLIOManager(
    {
        "host": os.getenv("MYSQL_HOST"),
        "port": 3306,
        "database": os.getenv("MYSQL_DATABASE"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
    }
)

minio = MinIOIOManager(
    {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "bucket": os.getenv("DATALAKE_BUCKET"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }
)

postgres = PostgreSQLIOManager(
    {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
)