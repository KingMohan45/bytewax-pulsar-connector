# Makefile for bytewax-pulsar connector local development

# Variables
PYTHON_VERSION := 3.12
PACKAGE_PATH := src/bytewax_pulsar_connector
PROJECT_NAME := bytewax-pulsar-connector
SONAR_HOST_URL := http://localhost:9000

# LOG_LEVEL := DEBUG
LOG_LEVEL := INFO


# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

.PHONY: help install lint test coverage sonar-local clean ci-check all producer

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

install: ## Install dependencies using Poetry
	@echo "$(YELLOW)Installing dependencies...$(NC)"
	@command -v poetry >/dev/null 2>&1 || { echo "$(RED)Poetry is not installed. Please install Poetry first.$(NC)" >&2; exit 1; }
	@poetry install --with dev --no-root
	@echo "$(GREEN)Dependencies installed successfully!$(NC)"

lint: ## Run pylint on the package
	@echo "$(YELLOW)Running pylint...$(NC)"
	@poetry run pylint $(PACKAGE_PATH) --max-line-length=120 || (echo "$(RED)Linting failed!$(NC)" && exit 1)
	@echo "$(GREEN)Linting passed!$(NC)"

test: ## Run pytest with coverage
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	@poetry run pytest --cov --cov-report=term --cov-report=xml --junitxml=report.xml
	@echo "$(GREEN)Tests completed!$(NC)"

coverage: test ## Generate and display coverage report
	@echo "$(YELLOW)Coverage Report:$(NC)"
	@poetry run coverage report
	@echo ""
	@echo "$(YELLOW)Detailed coverage saved to:$(NC)"
	@echo "  - coverage.xml (for SonarQube)"
	@echo "  - htmlcov/index.html (for browser viewing)"
	@poetry run coverage html

sonar-local: ## Run SonarQube scanner locally (requires local SonarQube instance)
	@echo "$(YELLOW)Running SonarQube analysis locally...$(NC)"
	@if command -v sonar-scanner >/dev/null 2>&1; then \
		sonar-scanner \
			-Dsonar.login=sqa_5ae55cb04520631408059f35619069a5d46373dd \
			-Dsonar.log.level=$(LOG_LEVEL) \
			-Dsonar.showProfiling=true \
			-Dsonar.scanner.dumpToFile=sonar-issues.json \
			-Dsonar.issuesReport.html.enable=true \
			-Dsonar.issuesReport.console.enable=true \
			-Dsonar.host.url=$(SONAR_HOST_URL) \
			-Dsonar.python.coverage.reportPaths=coverage.xml; \
		echo "$(GREEN)SonarQube analysis completed!$(NC)"; \
	else \
		echo "$(YELLOW)sonar-scanner not found. Skipping SonarQube analysis.$(NC)"; \
		echo "To install: brew install sonar-scanner (macOS) or download from SonarQube website"; \
	fi

clean: ## Clean up generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	@rm -rf .coverage coverage.xml report.xml htmlcov .pytest_cache
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleanup completed!$(NC)"

ci-check: clean install lint test ## Run all CI checks locally (clean, install, lint, test)
	@echo ""
	@echo "$(GREEN)=== All CI checks passed! ===$(NC)"
	@echo "Your code is ready to be pushed to GitLab CI"

all: ci-check ## Alias for ci-check

# Quick commands for development
quick-test: ## Run tests without coverage (faster)
	@poetry run pytest

watch-test: ## Run tests in watch mode (requires pytest-watch)
	@poetry run ptw -- --testmon

format: ## Format code using black and isort
	@echo "$(YELLOW)Formatting code...$(NC)"
	@poetry run black $(PACKAGE_PATH) 2>/dev/null || echo "$(YELLOW)black not installed, skipping...$(NC)"
	@poetry run isort $(PACKAGE_PATH) 2>/dev/null || echo "$(YELLOW)isort not installed, skipping...$(NC)"
	@echo "$(GREEN)Code formatting completed!$(NC)"

producer: ## Run interactive Pulsar producer for testing
	@echo "$(YELLOW)Starting Interactive Pulsar Producer...$(NC)"
	@poetry run python examples/interactive_producer.py

# Docker commands for local testing (optional)
docker-build-local: ## Build Docker image locally
	@echo "$(YELLOW)Building Docker image locally...$(NC)"
	@docker build -t $(PROJECT_NAME):local .
	@echo "$(GREEN)Docker image built successfully!$(NC)"

docker-run-local: ## Run Docker container locally
	@docker run --rm -it $(PROJECT_NAME):local

# Pulsar local setup
pulsar-start: ## Start Pulsar locally using Docker
	@echo "$(YELLOW)Starting Apache Pulsar in standalone mode...$(NC)"
	@docker run -d --name pulsar-standalone \
		-p 6650:6650 \
		-p 8080:8080 \
		apachepulsar/pulsar:latest \
		bin/pulsar standalone ; \
	@echo "$(GREEN)Pulsar started!$(NC)"
	@echo "  - Service URL: pulsar://localhost:6650"
	@echo "  - Admin URL: http://localhost:8080"

pulsar-stop: ## Stop local Pulsar container
	@echo "$(YELLOW)Stopping Pulsar...$(NC)"
	@docker stop pulsar-standalone 2>/dev/null || true
	@docker rm pulsar-standalone 2>/dev/null || true
	@echo "$(GREEN)Pulsar stopped!$(NC)"

pulsar-logs: ## View Pulsar container logs
	@docker logs -f pulsar-standalone

# Testing targets using pytest command-line options (no environment variables needed)
test-unit: ## Run unit tests with mocked Pulsar
	@echo "$(YELLOW)Running unit tests with mocked Pulsar...$(NC)"
	@poetry run pytest --tb=short
	@echo "$(GREEN)Unit tests completed!$(NC)"

test-integration: ## Run integration tests (may require running Pulsar)
	@echo "$(YELLOW)Running integration tests (may require running Pulsar)...$(NC)"
	@poetry run pytest -m integration --tb=short
	@echo "$(GREEN)Integration tests completed!$(NC)"

test-performance: ## Run performance tests
	@echo "$(YELLOW)Running performance tests...$(NC)"
	@poetry run pytest -m performance --tb=short
	@echo "$(GREEN)Performance tests completed!$(NC)"

test-all: ## Run all tests (unit + integration + performance)
	@echo "$(YELLOW)Running all tests...$(NC)"
	@poetry run pytest --tb=short
	@echo "$(GREEN)All tests completed!$(NC)"

test-coverage: ## Run tests with coverage report
	@echo "$(YELLOW)Running tests with coverage...$(NC)"
	@poetry run pytest --cov=src/bytewax_pulsar_connector --cov-report=html --cov-report=term-missing --tb=short
	@echo "$(GREEN)Coverage report generated!$(NC)"
	@echo "  - Open htmlcov/index.html in browser for detailed report"

test-debug: ## Run tests in debug mode with verbose output
	@echo "$(YELLOW)Running tests in debug mode...$(NC)"
	@poetry run pytest -vv -s --tb=long --capture=no

clean-test: ## Clean test artifacts
	@echo "$(YELLOW)Cleaning test artifacts...$(NC)"
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -rf .coverage
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Test artifacts cleaned!$(NC)"
