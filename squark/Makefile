IMAGE_NAME = squark-tests 

default: build

help:			## Prints the names and descriptions of available targets
	@echo ''; grep -h '\s\+##' $(MAKEFILE_LIST) | sed -e "s/:.*##/:/" | awk "{ task=\$$1; \$$1=\"\"; printf(\"%-12s %s\n\", task, \$$0); }"; echo ''

build:			## Build a docker image to test squark
	docker build --force-rm -t $(IMAGE_NAME) .

test_travis:    ## Test command for travis-ci.org 
	./scripts/travistests

coverage: 
	build coverageonly down			## Calculate the code coverage of the squark library

up:
	docker-compose up -d --build hdfs vertica postgres

down:			## Stop the services.
	docker-compose down
	docker-compose rm -f

test:			## Run tests, assuming the services are all up.
	./scripts/dockertests

coverageonly:			## Run coverage, assuming the services are all up.
	docker-compose run -u "$(shell id -u):$(shell id -g)" --rm coverager

publish: test			## Publish the built package with the python package index (pypi)
	if [ "$(GIT_BRANCH)" = 'origin/master' ]; then \
		docker-compose run --rm publisher; \
	fi
