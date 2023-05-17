from typing import Generator
from psycopg2.extensions import connection as _connection


class PostgresMerger:

    def __init__(self, pg_conn: _connection):
        self.conn = pg_conn
        self.cursor = pg_conn.cursor()

    def merge_records(self, fw_ids: list[str]) -> Generator:
        ids_str = ', '.join(['\''+id+'\'' for id in fw_ids])
        query = f'''
                    SELECT
                    fw.id as fw_id, 
                    fw.title, 
                    fw.description, 
                    fw.rating, 
                    fw.type, 
                    fw.created, 
                    fw.modified, 
                    COALESCE (
                               json_agg(
                               DISTINCT jsonb_build_object(
                                   'person_role', pfw.role,
                                   'person_id', p.id,
                                   'person_name', p.full_name
                               )
                               ) FILTER (WHERE p.id is not null),
                               '[]'
                    ) as persons,
                    array_agg(DISTINCT g.name) as genres
                    FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g ON g.id = gfw.genre_id
                    WHERE fw.id IN ({ids_str})
                    GROUP BY fw.id
                    ORDER BY fw.modified;     
                '''
        self.cursor.execute(query)
        for row in self.cursor:
            yield row
