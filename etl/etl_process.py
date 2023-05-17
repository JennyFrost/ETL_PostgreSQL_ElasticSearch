from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor

from etl_logging import logger
from sleep_func import sleep_func, backoff
import state
from settings import PostgresSettings, ElasticSettings

from postgres_extract.data_validator import Filmwork
from postgres_extract.postgres_producer import PostgresProducer
from postgres_extract.postgres_enricher import PostgresEnricher
from postgres_extract.postgres_merger import PostgresMerger
from elastic_load.es_loader import ESLoader


class ETLProcess:

    def __init__(self, time_to_sleep: int, pack_size: int, dsl: dict, es_path: list,
                 es_settings_path: str = 'elastic_load/es_settings.json',
                 index_name: str = 'movies'):
        self.time_to_sleep = time_to_sleep
        self.dsl = dsl
        self.es_path = es_path
        self.es_settings_path = es_settings_path
        self.pack_size = pack_size
        self.index_name = index_name

    @contextmanager
    def conn_context(self):
        conn = psycopg2.connect(**self.dsl, cursor_factory=DictCursor)
        try:
            yield conn
        finally:
            conn.close()

    @backoff()
    def extract(self, time_to_start: int, ids: list[str]) -> list[Filmwork] | None:
        with self.conn_context as pg_conn:
            producer = PostgresProducer(time_to_start, self.pack_size, pg_conn)
            tables_modified = producer.check_modified()
            if not tables_modified:
                logger.info(f'Nothing changed! Check again in {self.time_to_sleep} seconds')
                return None
            tables_with_modified_records_ids = producer.extract_from_tables(time_to_start=time_to_start,
                                                                            ids=ids)
            enricher = PostgresEnricher(self.pack_size, pg_conn)
            fws_ids = enricher.enrich_persons_genres(tables_with_modified_records_ids)
            merger = PostgresMerger(pg_conn)
            all_recs = merger.merge_records(fws_ids)

        filmworks = []
        for rec in all_recs:
            row_dict = dict(rec)
            filmworks.append(Filmwork(**row_dict))
        logger.info(f'{len(filmworks)} records extracted from postgres')
        return filmworks

    def transform(self, filmworks: list[Filmwork]) \
            -> list[dict[str: str|dict[str: str|float|dict[str: str]]]]:
        to_es = []
        for fw in filmworks:
            fw_dict = defaultdict(list)
            fw_dict['id'] = str(fw.fw_id)
            fw_dict['title'] = fw.title
            fw_dict['description'] = fw.description
            fw_dict['imdb_rating'] = fw.rating
            fw_dict['genre'] = fw.genres
            for person in fw.persons:
                fw_dict['director'] = []
                if person.person_role == 'director':
                    fw_dict['director'].append(person.person_name)
                for role in ('actor', 'writer'):
                    if person.person_role == role:
                        fw_dict[role+'s_names'].append(person.person_name)
                        person_dict = {'id': str(person.person_id), 'name': person.person_name}
                        fw_dict[role+'s'].append(person_dict)
            out_dict = {"index": {
                        '_index': self.index_name,
                        '_id': fw_dict['id']
                        }}
            to_es.extend([out_dict, dict(fw_dict)])

        return to_es

    @backoff()
    def load(self, data: list[dict[str: str | list[dict[str: str]]]], is_first_time: bool) -> dict[str]:
        loader = ESLoader(self.es_path, self.es_settings_path, self.index_name)
        if is_first_time:
            loader.create_index()
        return loader.load_data(data)

    @sleep_func(time_to_sleep=120)
    def etl(self) -> dict[str] | None:
        storage = state.JsonFileStorage(file_path='current_state.json')
        cur_state_dict = storage.retrieve_state()
        cur_state = state.State(storage)
        is_first_time = False
        if not cur_state_dict:
            is_first_time = True
            time_to_start = datetime.now() - timedelta(days=365)
            time_to_start = time_to_start.strftime("%Y-%m-%d %H:%M:%S")
        else:
            if 'last_date' in cur_state_dict:
                time_to_start = cur_state.get_state('last_date')
            else:
                time_to_start = cur_state.get_state('finished_at')
        filmworks = self.extract(time_to_start=time_to_start, ids=[])
        if not filmworks:
            return
        to_es = self.transform(filmworks)
        return self.load(to_es, is_first_time)


if __name__ == '__main__':

    dsl = PostgresSettings().dict()
    es_path = [ElasticSettings().dict()]
    etl_process = ETLProcess(time_to_sleep=20, pack_size=100, dsl=dsl, es_path=es_path)
    etl_process.etl()
