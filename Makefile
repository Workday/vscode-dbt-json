install:
	source .venv/bin/activate && python3.11 -m pip install -r airflow/2.10/requirements.txt
test:
	source .venv/bin/activate && python3.11 -m unittest airflow/2.10/utils.py