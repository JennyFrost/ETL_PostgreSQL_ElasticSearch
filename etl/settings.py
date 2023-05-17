from pydantic import BaseSettings, Field


class PostgresSettings(BaseSettings):

    dbname: str = Field(..., env="PG_NAME")
    user: str = Field(..., env="PG_USER")
    password: str = Field(..., env="PG_PASSWORD")
    host: str = Field(..., env="PG_HOST")
    port: str = Field(..., env="PG_PORT")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


class ElasticSettings(BaseSettings):

    scheme: str = Field(..., env="ES_SCHEME")
    host: str = Field(..., env="ES_HOST")
    port: int = Field(..., env="ES_PORT")

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
