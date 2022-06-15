FROM python:3.10

ENV DB_URL = nonsense
ENV UPDATE_FREQUENCY_MIN 15
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code/

# Install dependencies
COPY /requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./ /code/