MOON ?= $(shell which moon 2>/dev/null || echo $(HOME)/.moon/bin/moon)
PYTHON ?= python3

.PHONY: all check test run bench verify dashboard generator clean fmt

all: check test run

check:
	$(MOON) check

test:
	$(MOON) test

run:
	$(MOON) run cmd/main

bench:
	$(PYTHON) bench/bench_harness.py
	$(PYTHON) bench/latency_profile.py

verify:
	$(PYTHON) tools/verifier.py --count 10000

dashboard:
	$(PYTHON) tools/dashboard.py --static

generator:
	$(PYTHON) tools/generator.py --domain market --count 10

clean:
	rm -rf target _build *.generated.mbti */pkg.generated.mbti
