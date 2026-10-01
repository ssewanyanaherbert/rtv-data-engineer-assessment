COMPOSE := docker compose
SCHEDULER := $(COMPOSE) exec airflow-scheduler

.PHONY: up run test dashboard report psql logs down clean

up:            ## build images and start Postgres + Airflow
	$(COMPOSE) up -d --build

run:           ## run the whole DAG (bronze -> silver -> gold -> tests -> report) and wait for it
	$(SCHEDULER) airflow dags test rtv_survey_pipeline

test:          ## unit tests (PySpark + SQL test metadata)
	$(SCHEDULER) python -m pytest

dashboard:     ## open the Streamlit dashboard
	@echo "http://localhost:$${DASHBOARD_HOST_PORT:-8501}"

report:        ## print the latest run report
	cat reports/pipeline_report.md

psql:          ## open a SQL shell on the warehouse
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

logs:
	$(COMPOSE) logs -f airflow-scheduler

down:
	$(COMPOSE) down

clean:         ## stop and delete volumes and generated lake/report files
	$(COMPOSE) down -v
	find lake reports -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
