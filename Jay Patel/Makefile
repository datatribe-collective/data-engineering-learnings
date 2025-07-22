run:
 
	python3 scripts/clean_data.py
	python3 scripts/Run_Analysis_Notebook.py

airflow:
	airflow dags trigger zepto_pipeline_dag

.PHONY: clean

clean:
	rm -rf __pycache__ *.pyc .ipynb_checkpoints notebooks/*_output.ipynb