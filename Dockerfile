FROM python:3.13-slim

RUN pip install flake8

COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY /src /code/src/
COPY /tests /code/tests/
# The config schema is a test fixture, not a run-time input: nothing in src/ reads it. The schema tests
# guard the invariant that the schema default and DEFAULT_LOGIN_METHOD stay equal, and they can only do
# that inside the image if the file is here.
COPY /component_config /code/component_config/
COPY /scripts /code/scripts/
COPY requirements.txt /code/requirements.txt
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]
