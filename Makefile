# Berth. `make install && make test`

PY      ?= .venv/bin/python
VENV_PY ?= python3.12          # NOT python3: that is 3.6 on some machines

.PHONY: help install test demo bench clean

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

install: ## create .venv with the test tools (Berth itself has no dependencies)
	$(VENV_PY) -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -r requirements-dev.txt

test: ## the whole suite, over real sockets
	$(PY) -m pytest -q

bench: ## head to head against nginx, cores pinned (needs Docker)
	docker pull -q nginx:alpine
	docker pull -q williamyeh/wrk
	$(PY) -m bench.versus_nginx --duration 15 --connections 64

demo: ## proxy two local nginx backends on :8080, stats on :8081
	@echo "start backends first, e.g. the bench backend container, then:"
	$(PY) -m berth.cli --backend 127.0.0.1:9101 --backend 127.0.0.1:9102 --health-path /

clean:
	rm -rf .venv .pytest_cache bench/.run **/__pycache__
