FROM python

WORKDIR /usr/src/app

RUN python -m pip install kafka-python && python -m pip install psycopg2

CMD [ "python", "print-kafka.py" ]

