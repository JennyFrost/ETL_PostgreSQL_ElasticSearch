import json
from elastic_transport import ObjectApiResponse
from elasticsearch import Elasticsearch

from etl_logging import logger


class ESLoader:

    def __init__(self, es_path: list[dict], settings_path: str, index_name: str):
        self.es = Elasticsearch(es_path)
        self.settings_path = settings_path
        self.index_name = index_name

    def create_index(self):
        with open(self.settings_path, 'r') as f:
            settings = json.load(f)
        if self.es.indices.exists(index=self.index_name):
            logger.info('Failed to create - index already exists')
            return {'error': f'index {self.index_name} already exists'}
        logger.info('Creating index')
        self.es.indices.create(index=self.index_name, body=settings)
        return {'successful': f'{self.index_name}'}

    def apply_bulk(self, actions: list[dict[str: str | dict[str: str|float|dict[str: str]]]]) \
            -> ObjectApiResponse | dict[str: str]:
        try:
            logger.info('Applying bulk')
            return self.es.bulk(operations=actions, refresh=True)
        except Exception as e:
            logger.error(f'Bulk operation failed. Error: {e}')
            return {'error': str(e)}

    def delete_previous(self, ids: list[str]) -> ObjectApiResponse:
        actions = [{"delete": {"_index": self.index_name, "_id": id}} for id in ids]
        return self.apply_bulk(actions=actions)

    def load_data(self, actions: list[dict[str: str | dict[str: str|float|dict[str: str]]]]) -> dict[str: list[str]]:
        oks, fails = [], []
        if actions:
            resp_bulk = self.apply_bulk(actions=actions)
            if 'error' in resp_bulk or ('errors' in resp_bulk and resp_bulk['errors'] is True):
                logger.error(str(resp_bulk))
            else:
                for operation_report in resp_bulk['items']:
                    if operation_report['index']['status'] == 201:
                        oks.append(operation_report['index']['_id'])
                    else:
                        fails.append(operation_report['index']['_id'])

        return {
            'successful': oks,
            'failed': fails
        }
