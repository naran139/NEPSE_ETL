FROM apache/airflow:2.10.3-python3.12

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/app"
ENV PYTHONPATH="${PYTHONPATH}}:/opt/airflow/wedriver"

COPY requirements.txt .
COPY webdriver/chromedriver.exe . 

USER airflow

RUN pip install selenium && \
    pip install bs4 && \
    pip install lxml && \
    pip install selenium-stealth