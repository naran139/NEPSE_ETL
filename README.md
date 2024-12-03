NEPSE DATA PIPELINE AND VISUALIZING USING LOOKER STUDIO

HOW TO USE:

1. Create Virtual env
    python -m venv venv

2. Install dependencies inside env
    activate venv first and pip install -r requirements.txt

3. Your should have Docker Desktop installed in your PC
    -Initialize the airflow-init
        docker compose up airflow-init
    -Run the docker container
        docker compose up

4. Open the localhost:8080 in your browser
    id :-airflow
    pw :-airflow

5. Search for the NEPSE_DAG and execute