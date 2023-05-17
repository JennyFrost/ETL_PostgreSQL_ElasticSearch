FROM python:3.10

WORKDIR /opt/etl

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install wheel --no-cache-dir \
     && pip install --upgrade setuptools \
     && pip install -r requirements.txt --no-cache-dir

COPY . .

EXPOSE 8000

ENTRYPOINT ["python3", "etl_process.py"]