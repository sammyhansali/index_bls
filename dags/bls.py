# General imports

# Airflow imports
from airflow.sdk import dag, task

@dag
def bls():
    
    @task
    def extract():
        pass

    @task
    def transform():
        pass

    @task
    def load():
        pass

    extract() >> transform() >> load()

bls()