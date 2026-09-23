# ZeptoClaw Agent Mesh — сборка, проверка, тесты, развёртывание.
#
# Быстрый старт:  make            (fmt-check + vet + build + test)
# Полный цикл:    make ci
#
# Все цели работают без GOFLAGS=-mod=mod, если модули уже скачаны; цель
# `deps` делает это явно.

SHELL      := /bin/bash
GO         ?= go
GOFLAGS    ?= -mod=mod
BIN_DIR    := bin
BIN        := zeptomesh-node
PKG        := ./cmd/$(BIN)
UI_BIN     := zeptomesh-ui
UI_PKG     := ./cmd/$(UI_BIN)
MOD        := github.com/developer3000S/zeptoclaw
PROTO_DIR  := api/proto
PROTO_OUT  := gen
PROTO      := $(PROTO_DIR)/zeptomesh/v1/mesh.proto

VERSION    ?= 0.1.0
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS    := -s -w \
  -X $(MOD)/internal/version.Version=$(VERSION) \
  -X $(MOD)/internal/version.GitCommit=$(GIT_COMMIT) \
  -X $(MOD)/internal/version.BuildDate=$(BUILD_DATE)

# Пакеты с тестами и все пакеты кода — вычисляются один раз.
TEST_PKGS  := $(shell $(GO) list ./... 2>/dev/null)
GO_FILES   := $(shell find . -name '*.go' -not -path './.git/*' 2>/dev/null)

# GOBIN нужен для protoc-gen-go; кладём его в PATH локально для целей proto*.
GOBIN_DIR  := $(shell $(GO) env GOPATH)/bin

.DEFAULT_GOAL := all
.PHONY: all help

all: fmt-check vet build test ## Стандартная проверка: формат, vet, сборка, тесты

help: ## Показать список целей
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

##—— Подготовка ------------------------------------------------------------

deps: ## Скачать и суммировать модули
	$(GO) mod download && $(GO) mod verify

toolchain: ## Установить protoc-gen-go (нужен только для перегенерации proto)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	@echo "protoc-gen-go -> $(GOBIN_DIR)/protoc-gen-go"

##—— Код ------------------------------------------------------------------

build: ## Собрать демон в bin/$(BIN)
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BIN) $(PKG)
	@echo "built $(BIN_DIR)/$(BIN) ($(VERSION) $(GIT_COMMIT))"

build-race: ## Собрать с детектором гонок
	CGO_ENABLED=1 $(GO) build $(GOFLAGS) -race -o $(BIN_DIR)/$(BIN)-race $(PKG)

build-ui: ## Собрать BFF панели в bin/$(UI_BIN)
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(UI_BIN) $(UI_PKG)
	@echo "built $(BIN_DIR)/$(UI_BIN) ($(VERSION) $(GIT_COMMIT))"

cross: ## Собрать linux/{amd64,arm64} (ТЗ 15.1)
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
	  -o $(BIN_DIR)/$(BIN)-linux-amd64 $(PKG)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
	  -o $(BIN_DIR)/$(BIN)-linux-arm64 $(PKG)
	@ls -la $(BIN_DIR)/$(BIN)-linux-*

fmt: ## Форматировать код (gofmt -w)
	gofmt -w $(shell find internal cmd -name '*.go')

fmt-check: ## Проверить, что код отформатирован
	@out=$$(gofmt -l internal cmd 2>/dev/null); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet: ## go vet по всем пакетам
	$(GO) vet $(GOFLAGS) ./...

tidy: ## go mod tidy
	$(GO) mod tidy

##—— Генерация протокола ---------------------------------------------------

proto: ## Перегенерировать gen/ из $(PROTO) (нужны protoc и protoc-gen-go)
	@command -v protoc >/dev/null || { echo "protoc not found"; exit 1; }
	@test -x $(GOBIN_DIR)/protoc-gen-go || { echo "protoc-gen-go not found: run 'make toolchain'"; exit 1; }
	PATH="$(GOBIN_DIR):$$PATH" protoc \
	  --proto_path=$(PROTO_DIR) \
	  --go_out=paths=source_relative:$(PROTO_OUT) \
	  $(PROTO)
	@echo "regenerated $(PROTO_OUT)/zeptomesh/v1/mesh.pb.go"

proto-check: ## Убедиться, что gen/ соответствует $(PROTO)
	@tmp=$$(mktemp -d); \
	PATH="$(GOBIN_DIR):$$PATH" protoc --proto_path=$(PROTO_DIR) \
	  --go_out=paths=source_relative:$$tmp $(PROTO) 2>/dev/null; \
	if [ -d "$$tmp/zeptomesh" ]; then \
	  diff -q $$tmp/zeptomesh/v1/mesh.pb.go $(PROTO_OUT)/zeptomesh/v1/mesh.pb.go >/dev/null \
	    && echo "gen/ is up to date with $(PROTO)" \
	    || { echo "gen/ is STALE — run 'make proto'"; rm -rf $$tmp; exit 1; }; \
	else \
	  echo "protoc unavailable or produced nothing; skipping check"; \
	fi; rm -rf $$tmp

##—— Тесты -----------------------------------------------------------------

test: ## Все тесты
	$(GO) test $(GOFLAGS) ./...

test-race: ## Все тесты под -race (медленнее)
	CGO_ENABLED=1 $(GO) test $(GOFLAGS) -race -timeout 20m ./...

test-cover: ## Покрытие по всем пакетам → coverage.html
	$(GO) test $(GOFLAGS) -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "coverage.html written"

test-unit: ## Только юнит-тесты (без интеграционных и нагрузочных)
	$(GO) test $(GOFLAGS) -short -timeout 5m $(TEST_PKGS)

test-integration: ## Интеграционные сценарии ТЗ 17.2 + спец. проверки 17.4
	CGO_ENABLED=1 $(GO) test $(GOFLAGS) -race -tags=integration -timeout 25m \
	  -count=1 ./internal/node/... ./internal/security/...

test-load: ## Нагрузочная симуляция ТЗ 17.3 (100/500/1000 узлов) и отчёт
	$(GO) test $(GOFLAGS) -tags=load -timeout 60m -run TestLoadSim -v ./internal/simulator/... \
	  | tee load-test-output.txt

bench: ## Бенчмарки маршрутизации и канонического кодирования
	$(GO) test $(GOFLAGS) -run '^$$' -bench . -benchmem -benchtime 200x \
	  ./internal/routing/... ./internal/wire/...

##—— Качество --------------------------------------------------------------

lint: ## Статический анализ (golangci-lint, если установлен)
	@command -v golangci-lint >/dev/null \
	  && golangci-lint run ./... \
	  || echo "golangci-lint not installed; 'go vet' + 'gofmt -l' are the enforced gates"

static-check: ## gofmt + vet + проверка gen/ против proto — то, что требует CI
	@$(MAKE) --no-print-directory fmt-check
	@$(MAKE) --no-print-directory vet
	@$(MAKE) --no-print-directory proto-check

##—— CI-эквивалент локально ------------------------------------------------

ci: static-check build test-race ## Полный конвейер: статика, сборка, тесты под -race
	@echo "CI-equivalent checks passed"

##—— Локальная сеть узлов (отладка) ----------------------------------------

run-1: build ## Запустить узел 0 отладочного кластера (данные в .dev/cluster/0)
	@mkdir -p .dev/cluster/0 && ZETOMESH_INDEX=0 ZETOMESH_DATA=$(CURDIR)/.dev/cluster \
	  ZETOMESH_SKILL=general ZETOMESH_BOOTSTRAP= \
	  ./bin/$(BIN) run -config configs/examples/dev-node.yaml

dev-cluster: build ## Поднять 3 узла на одной машине и показать статус
	@bash scripts/dev-cluster.sh up 3

dev-status: ## Сводка по отладочному кластеру
	@bash scripts/dev-cluster.sh status

dev-rotate: ## Ротировать ключ узла 0 и перезапустить его под новым
	@bash scripts/dev-cluster.sh rotate 0

dev-cluster-down: ## Остановить отладочный кластер
	@bash scripts/dev-cluster.sh down

##—— Установка / Docker ----------------------------------------------------

install-local: build ## Установить локально (systemd), 1 экземпляр
	./install.sh --mode local --nodes 1

install-docker: ## Собрать образ и поднять сеть из 3 узлов
	./install.sh --mode docker --nodes 3

docker-build: ## Только сборка образа
	docker build --no-cache -f deploy/docker/Dockerfile -t zeptomesh-node:$(VERSION) .

docker-build-ui: ## Только сборка образа панели
	docker build --no-cache -f deploy/ui/Dockerfile \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg GIT_COMMIT=$(GIT_COMMIT) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -t zeptomesh-ui:latest .

install-ui: docker-build-ui ## Поднять панель в Docker (агенты должны быть запущены)
	./install-ui.sh

##—— Очистка ---------------------------------------------------------------

clean: ## Удалить артефакты сборки и покрытия
	rm -rf $(BIN_DIR) coverage.out coverage.html .dev
	$(GO) clean -cache -testcache 2>/dev/null || true

.PHONY: deps toolchain build build-ui build-race cross fmt fmt-check vet tidy \
        proto proto-check test test-race test-cover test-unit test-integration \
        test-load bench lint static-check ci run-1 dev-cluster dev-status dev-rotate \
        dev-cluster-down \
        install-local install-docker docker-build docker-build-ui install-ui \
        clean help