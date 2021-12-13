all: lint test

build:
	@rm -rf dist
	@poetry build

format:
	@poetry run black .

lint:
	@poetry run pylint ./pureskillgg_dsdk
	@poetry run black --check .

publish:
	@poetry run twine upload --skip-existing --repository-url https://pureskillgg.jfrog.io/artifactory/api/pypi/private dist/*

test:
	@poetry run pytest --cov=./pureskillgg_dsdk

watch:
	@poetry run ptw

.PHONY: build docs test
