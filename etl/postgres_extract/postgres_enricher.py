from typing import List
from psycopg2.extensions import connection as _connection


class PostgresEnricher:

    def __init__(self, size: int, pg_conn: _connection):
        self.size = size
        self.conn = pg_conn
        self.cursor = pg_conn.cursor()
        self.tables = ('genre_film_work', 'person_film_work')

    def enrich_persons_genres(self, tables_with_modified_records: dict) -> List[str]:
        fws_ids = []
        for table in tables_with_modified_records:
            if table in self.tables:
                ids = self.extract_filmworks(tables_with_modified_records[table], table, table+'_id')
                fws_ids.extend(ids)
            else:
                fws_ids.extend(tables_with_modified_records[table])
        return fws_ids

    def extract_filmworks(self, ids: List[str], table_to_join: str, column: str) -> List[str]:
        ids_str = ', '.join(['\''+id+'\'' for id in ids])
        fw_ids = []
        while True:
            query = f'''
                       SELECT fw.id
                       FROM content.film_work fw
                       LEFT JOIN content.{table_to_join} ON content.{table_to_join}.film_work_id = fw.id
                       WHERE content.{table_to_join}.{column} IN ({ids_str})
                       ORDER BY fw.modified
                       LIMIT {self.size}
                       OFFSET {self.size}
                    '''
            self.cursor.execute(query)
            recs = self.cursor.fetchall()
            if recs:
                fw_ids.extend([rec[0] for rec in recs])
            else:
                break
        return fw_ids
