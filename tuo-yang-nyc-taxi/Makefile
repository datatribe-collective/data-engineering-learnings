# Makefile

COMPOSE = docker compose

# ------------------------------
# Global Config
# ------------------------------
IMAGE_PREFIX ?= bravo634

# ------------------------------
# Local Development
# ------------------------------
up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

restart:
	$(COMPOSE) down && $(COMPOSE) up --build

# ------------------------------
# Python Checks
# ------------------------------
lint:
	flake8 streamlit_app airflow_pipeline

install:
	pip install -r streamlit_app/requirements.txt
	pip install -r airflow_pipeline/requirements.txt

# ------------------------------
# Build Images
# ------------------------------
build-airflow:
	docker build -t $(IMAGE_PREFIX)/nyc-taxi-airflow:latest ./airflow_pipeline

build-streamlit:
	docker build -t $(IMAGE_PREFIX)/nyc-taxi-streamlit:latest ./streamlit_app

build-all: build-airflow build-streamlit

# ------------------------------
# Push Images
# ------------------------------
push-airflow:
	docker push $(IMAGE_PREFIX)/nyc-taxi-airflow:latest

push-streamlit:
	docker push $(IMAGE_PREFIX)/nyc-taxi-streamlit:latest

push-all: push-airflow push-streamlit

# ------------------------------
# One Command for Build + Push
# ------------------------------
publish: build-all push-all