run:
	python3 scripts/clean_data.py
	python3 scripts/run_notebook.py

# Airflow and jupyter notebook are not compatible on my computer as they need 
# different versions of httpx, but I'm including this airflow instruction anyway
airflow:
	airflow dags trigger covid_data_analysis_pipeline

.PHONY: clean

clean:
	rm -rf output/*.ipynb __pycache__ *.pyc .ipynb_checkpoints data/cleaned_covid.csv
