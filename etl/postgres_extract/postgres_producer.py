from uuid import UUID
import psycopg2
from psycopg2.extensions import connection as _connection

from sleep_func import backoff_break
from etl_logging import logger


class PostgresProducer:

    def __init__(self, time_to_start: int, size: int, pg_conn: _connection):
        self.time_to_start = time_to_start
        self.pack_size = size
        self.conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.tables_to_check = ('film_work', 'genre', 'person')

    def check_modified(self) -> dict:
        tables_modified = {}
        for table in self.tables_to_check:
            query = f'''
                        SELECT COUNT(*)
                        FROM content.{table}
                        WHERE modified > '\''{self.time_to_start}'\''::timestamp with time zone
                    '''
            self.cursor.execute(query)
            num_records = self.cursor.fetchone()
            if num_records[0]:
                tables_modified[table] = num_records[0]
        return tables_modified

    def extract_from_tables(self, time_to_start: str, ids: list[str]) -> dict[str: list[UUID]]:
        logger.info(f'Select dates starting from {self.time_to_start}')
        tables_with_modified_records_ids = {}
        tables_modified = self.check_modified()
        for table in tables_modified:
            num_records = tables_modified[table]
            if num_records > 0:
                tables_with_modified_records_ids[table] = self.extract_new_records(table,
                                                                                   time_to_start=time_to_start,
                                                                                   ids=ids)
        return tables_with_modified_records_ids

    @backoff_break()
    def extract_new_records(self, table: str, time_to_start: str, was_error: bool = False,
                            ids: list[str] =[]) -> list[str] | tuple[str, list[str], str, bool]:
        n = 0
        last_date = 0
        if not was_error:
            time_to_start = self.time_to_start
        while True:
            try:
                query = f'''
                            SELECT id, modified
                            FROM content.{table}
                            WHERE modified > '\''{time_to_start}'\''::timestamp with time zone
                            ORDER BY modified
                            LIMIT {self.pack_size}
                            OFFSET {n * self.pack_size}
                        '''
                self.cursor.execute(query)
                recs = self.cursor.fetchall()
                if recs:
                    ids.extend([rec[0] for rec in recs])
                    n += 1
                    last_date = recs[-1][1]
                else:
                    break
            except psycopg2.OperationalError:
                logger.error('Database does not respond! Trying again')
                return 'error', ids, last_date, True
        return ids
