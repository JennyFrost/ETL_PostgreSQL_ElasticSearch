from datetime import datetime
from functools import wraps
from typing import List

import elasticsearch
import psycopg2
import time
from tzlocal import get_localzone

import state
from etl_logging import logger


def sleep_func(time_to_sleep: int):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            while True:
                storage = state.JsonFileStorage(file_path='current_state.json')
                cur_state = state.State(storage)
                func(*args, **kwargs)
                end = datetime.now().astimezone(get_localzone()).strftime("%Y-%m-%d %H:%M:%S")
                cur_state.set_state('finished_at', end)
                time.sleep(time_to_sleep)

        return inner
    return func_wrapper


def backoff_break(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(self, *args, time_to_start: int, ids: List[str]):
            t = start_sleep_time
            n = 0
            storage = state.JsonFileStorage(file_path='current_state.json')
            cur_state = state.State(storage)
            while True:
                if t < border_sleep_time:
                    t *= factor ** n
                else:
                    t = border_sleep_time
                value = func(self, *args, time_to_start=time_to_start, ids=ids)
                if 'error' in value:
                    time.sleep(t)
                    n += 1
                    ids = value[1]
                    time_to_start = value[2]
                    cur_state.set_state('last_date', time_to_start)
                else:
                    return value

        return inner
    return func_wrapper


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            n = 0
            while True:
                if t < border_sleep_time:
                    t *= factor ** n
                else:
                    t = border_sleep_time
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, elasticsearch.exceptions.ConnectionError,
                        elasticsearch.exceptions.ConnectionTimeout):
                    logger.error('Database does not respond! Trying again')
                    time.sleep(t)
                    n += 1
        return inner
    return func_wrapper
