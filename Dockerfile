FROM apache/airflow:2.9.0
USER airflow
COPY requirement.txt /Documents/airflow-project/requirement.txt
RUN pip3 install -r /Documents/airflow-project/requirement.txt
# RUN pip install --no-cache-dir --user /Documents/airflow-project/requirement.txt