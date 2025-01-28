FROM python:3.13-slim

RUN pip install flake8

COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY requirements.txt /code/requirements.txt
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]
