.PHONY: dockerbuild dockerpush test testonce ruff pylint black lint
VERSION ?= latest
IMAGENAME = harvest-transformer
DOCKERREPO ?= 312280911266.dkr.ecr.eu-west-2.amazonaws.com

dockerbuild:
	DOCKER_BUILDKIT=1 docker build -t ${IMAGENAME}:${VERSION} .

dockerpush: dockerbuild testdocker
	docker tag ${IMAGENAME}:${VERSION} ${}DOCKERREPO}/${IMAGENAME}:${VERSION}
	docker push ${DOCKERREPO}/${IMAGENAME}:${VERSION}

test:
	./venv/bin/ptw pytest

testonce:
	./venv/bin/pytest

pylint:
	./venv/bin/pylint CHANGEME-package-names

ruff:
	./venv/bin/ruff .

black:
	./venv/bin/black .

lint: ruff black
