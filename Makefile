.PHONY: install demo test api health export lambda-zip
install:
	pip install -r requirements.txt && pip install -e .
demo:            ## whole pipeline locally on synthetic data
	python -m dre.cli demo
test:
	python -m pytest -q
api:
	uvicorn dre.api.main:app --reload
health:
	python -m dre.cli health
export:          ## CSVs for Power BI / Tableau
	python -m dre.cli export --out data/export
lambda-zip:      ## deployable Lambda package (stdlib + boto3 which Lambda provides)
	mkdir -p lambda_build && cp src/dre/streaming/lambda_handler.py lambda_build/lambda_function.py \
	&& cd lambda_build && zip -q ../infra/terraform/lambda.zip lambda_function.py
