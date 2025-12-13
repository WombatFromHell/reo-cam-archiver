PY = python3
SRC_DIR = src
BUILD_DIR = dist
STAGING = .build
ENTRY = src.entry:main
ARTIFACT = archiver.pyz
OUT = $(BUILD_DIR)/$(ARTIFACT)

build:
	mkdir -p $(BUILD_DIR)
	rm -rf $(STAGING)
	mkdir -p $(STAGING)
	cp -r $(SRC_DIR) $(STAGING)/
	$(PY) -m zipapp $(STAGING) -o $(OUT) -m $(ENTRY) -p "/usr/bin/env python3"
	chmod +x $(OUT)

test:
	uv run pytest -xvs --cov=src --cov-report=term-missing --cov-branch

lint:
	ruff check --select I ./src ./tests --fix; \
		pyright ./src ./tests

prettier:
	prettier --cache -c -w *.md

format: prettier
	ruff format ./src ./tests

radon:
	uv run radon cc ./src -a

quality: lint format

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +; \
	rm -rf \
		$(STAGING) \
		$(BUILD_DIR) \
		.pytest_cache \
		.ruff_cache \
		.coverage

all: clean build

.PHONY: build test lint prettier format radon quality clean all
