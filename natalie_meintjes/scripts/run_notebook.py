
import papermill as pm

pm.execute_notebook(
    'notebooks/covid_analysis.ipynb',
    'output/covid_analysis_output.ipynb'
)
