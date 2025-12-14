PY = python3
SRC_DIR = src
BUILD_DIR = dist
ENTRY = entry:main
ARTIFACT = archiver.pyz
OUT = $(BUILD_DIR)/$(ARTIFACT)

build:
	mkdir -p $(BUILD_DIR)
	$(PY) -m zipapp $(SRC_DIR) -o $(OUT) -m $(ENTRY) -p "/usr/bin/env python3"
	chmod +x $(OUT)
	cp -f ./archive-task.sh ./cleanup-task.sh $(BUILD_DIR)

test:
	uv run pytest -xvs --cov=src --cov-report=term-missing --cov-branch

lint:
	ruff check ./src ./tests; \
		pyright ./src ./tests

prettier:
	prettier --cache -c -w *.md

format: prettier
	ruff check --select I ./src ./tests --fix; \
	ruff format ./src ./tests

radon:
	uv run radon cc ./src -a

quality: lint format

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +; \
	rm -rf \
		$(BUILD_DIR) \
		.pytest_cache \
		.ruff_cache

all: clean build

.PHONY: build test lint prettier format radon quality clean all
