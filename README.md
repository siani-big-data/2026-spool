<div align="center">

<img src="assets/spool_icon.svg" alt="Spool Logo" width="220"/>

<br/>

# SPOOL

### *An agnostic, auditable & event-driven data ingestion framework*

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Build](https://img.shields.io/badge/build-Maven-red?logo=apachemaven)](https://maven.apache.org/)
[![Organization](https://img.shields.io/badge/org-spool--framework-181717?logo=github)](https://github.com/spool-framework)

</div>

---

> **SPOOL** is a Java-based, technology-agnostic framework for building scalable, auditable data ingestion pipelines.  
> It follows an **event-driven** and **ports & adapters**, exposing clean extension points via SPI so you can plug in any technology (Kafka, RabbitMQ, PostgreSQL, S3) without touching core logic.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Modules](#-modules)
  - [Crawler](#-crawler)
  - [Ingester](#-ingester)
  - [Validator](#-validator)
  - [Mounter](#-mounter)
  - [Janitor](#-janitor)
  - [Watchdog](#-watchdog)
  - [Core](#-core)
  - [Infrastructure](#-infrastructure)
  - [DSL](#-dsl)
- [Data Flow](#-data-flow)
- [YAML DSL](#-yaml-dsl)
- [Organization](#-organization)
- [License](#-license)

---

## 🔍 Overview

SPOOL decouples **data acquisition**, **validation**, **storage**, and **transformation** into independent, deployable modules.  
Each module communicates through an event bus, making the pipeline resilient, observable and horizontally scalable.

Key design goals:

- **Technology-agnostic** — no vendor lock-in; swap brokers, databases or storage layers via configuration
- **Auditable** — every event is identified by an `IdempotencyKey` (`sourceId + payload`) ensuring traceability and deduplication
- **Extensible** — custom validators, deserializers and infrastructure plugins loaded at runtime via Java `ServiceLoader` (SPI)
- **Observable** — built-in health check endpoints per module, compatible with ECS and Kubernetes probes

---

## 🏛 Architecture

SPOOL is structured around **hexagonal architecture** (Ports & Adapters), split into `api` and `internal` layers per module:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Crawler   │────▶│   Ingester  │────▶│  Data Lake  │────▶│   Mounter   │
│  (acquire)  │     │  (validate) │     │  (raw/cur.) │     │  (datamart) │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
       │                   │                                        │
       └───────────────────┴──────────── Event Bus ────────────────┘
                                              │
                              ┌───────────────┴───────────────┐
                        ┌─────────────┐               ┌─────────────┐
                        │   Watchdog  │               │   Janitor   │
                        │  (monitor)  │               │  (cleanup)  │
                        └─────────────┘               └─────────────┘
```

All inter-module communication is **event-driven**. The infrastructure (broker, inbox, data lake) is declared once in a shared YAML descriptor and injected into every module via the `infrastructure` SPI.

---

## 📦 Modules

### 🕷 Crawler

> Acquires raw data from external sources on a configurable schedule.

The Crawler polls HTTP endpoints (or any custom source) and emits raw records into the **inbox**.  
It uses a pipeline of `Deserializer → Splitter → Serializer` to normalize heterogeneous payloads (JSON objects, JSON arrays, nested paths) into discrete events before publishing.

- Configurable poll schedule (e.g. every N seconds)
- Supports `JSON_OBJECT`, `JSON_ARRAY` and `JSON_PATH` formats
- Fully decoupled from any serialization library via SPI factories
- Idempotency check before insertion using `sourceId + payload`

---

### ✅ Ingester

> Validates and persists incoming raw events to the Data Lake.

The Ingester consumes events from the inbox, runs them through a **chain of validators** discovered via SPI, and writes valid records to the raw Data Lake layer.  
Invalid events are quarantined rather than discarded, preserving full auditability.

- Validator chain ordered by priority
- Validators return pass/fail results — no exception-driven control flow
- Constructs S3 partition paths from the `PartitionKeySchema` embedded in each event

---

### 🔬 Validator

> Provides the contracts and annotations to define custom validators.

`validator` is a library module that defines the validation contract.  
By annotating your classes with the provided annotation, the Ingester automatically discovers and associates your validators to specific event types via `ServiceLoader` — no manual wiring needed.

- Annotation-driven validator registration
- SPI-based discovery — just annotate, implement and include in the classpath
- Validators are scoped to concrete event types

---

### 🗄 Mounter

> Transforms curated Data Lake data into queryable datamarts.

The Mounter reads from the **curated layer** of the Data Lake and produces **datamarts** optimised for downstream consumption (analytics, APIs, exports).  
It operates in batch mode, partitioned by `sourceId + eventType`, and delegates the transformation strategy to the user via SPI.

- Periodic polling with configurable interval
- User-supplied transformation strategies loaded via SPI
- Partition-aware batch processing

---

### 🧹 Janitor

> Keeps the inbox clean and ensures no envelope is silently lost.

The Janitor is a maintenance module that runs autonomously alongside the pipeline.  
It removes envelopes that have already been successfully persisted and republishes those that have been stuck for too long, acting as a self-healing mechanism against processing failures.

- Purges persisted envelopes from the inbox
- Detects and republishes stale/stuck envelopes
- Configurable staleness thresholds
- Complements the quarantine strategy of the Ingester

---

### 🐕 Watchdog

> Monitors the health of all running SPOOL modules.

The Watchdog is a **standalone service** that receives heartbeats from every module and exposes a unified health endpoint.  
It detects failures via missed heartbeat thresholds and can be deployed as a self-contained Docker image, ready for ECS or Kubernetes.

- REST endpoints for module registration and heartbeat
- Compatible with ECS health checks and Kubernetes liveness probes
- Embeds a lightweight HTTP server on a configurable port

---

### 🧱 Core

> Shared contracts, domain models and common utilities.

`core` is the foundation of the entire framework. It hosts the domain model, shared interfaces and utility classes used across all other modules.  
No module depends on another module's internals — they all depend on `core`.

---

### ⚙️ Infrastructure

> SPI and base providers for pluggable infrastructure.

`infrastructure` defines an SPI and an annotation that allows you to create **infrastructure plugins** — concrete implementations of brokers, databases, storage layers, etc.  
It also ships a set of base providers out of the box so you can get started without writing boilerplate.

- Annotation-driven plugin registration
- SPI-based discovery at runtime
- Base providers included for common technologies

---

### 📝 DSL

> Declarative YAML descriptor for module configuration.

The `dsl` module defines and validates the YAML configuration language used to describe any SPOOL node.  
It provides the parser, the configuration model, and the constraint validation so that misconfigured descriptors fail fast with clear error messages.

---

## 🌊 Data Flow

```
External Source
      │
      ▼
  [Crawler]
      │  raw records
      ▼
  [Raw Inbox]  ←── idempotency check       ◀── [Janitor] purges & republishes
      │
      ▼
  [Ingester]  ←── validator chain (SPI)
      │  valid events / quarantine
      ▼
  [Raw Data Lake] ←── Medallion Architecture
      │
      ▼
  [Mounter]  ←── transformation / aggregation strategy (SPI)
      │
      ▼
  [Datamart]
```

---

## 🗂 YAML DSL

SPOOL modules are configured through a **declarative YAML descriptor**.  
Infrastructure is declared once and shared across all modules in the node:

```yaml
infrastructure:
  watchdog:
    url: "http://localhost:8090"
  eventBus:
    type: "KAFKA"
    url:  "localhost:9092"
  inbox:
    sql:
      type:     "postgresql"
      host:     "localhost"
      database: "spool"
      user:     "spool"
      password: "spool"
  dataLake:
    type:   "S3"
    bucket: "spool-datalake"
    region: "eu-west-1"

spoolModuleList:
  crawler:
    - id: "my-crawler"
      source:
        id: "my-source"
        poll:
          http:
            url: "https://api.example.com/data"
          schedule:
            every: 60
            unit:  "SECONDS"
        format: "JSON_ARRAY"
      eventMapping:
        namingConvention: "SNAKE_CASE"
        attributeList:
          - value: "symbol"
```

---

## 🏢 Organization

SPOOL is distributed as a **collection of repositories** under the [`spool-framework`](https://github.com/spool-framework) GitHub organization.

| Repository | Description |
|---|---|
| [`core`](https://github.com/spool-framework/core) | Shared contracts, domain models and common utilities |
| [`infrastructure`](https://github.com/spool-framework/infrastructure) | SPI, annotation and base providers for pluggable infrastructure |
| [`dsl`](https://github.com/spool-framework/dsl) | Declarative YAML configuration language and parser |
| [`validator`](https://github.com/spool-framework/validator) | Contracts and annotations for SPI-based custom validators |
| [`crawler`](https://github.com/spool-framework/crawler) | Crawler module — data acquisition from external sources |
| [`ingester`](https://github.com/spool-framework/ingester) | Ingester module — validation and raw Data Lake persistence |
| [`mounter`](https://github.com/spool-framework/mounter) | Mounter module — curated layer to datamart transformation |
| [`janitor`](https://github.com/spool-framework/janitor) | Janitor module — inbox cleanup and stuck envelope recovery |
| [`watchdog`](https://github.com/spool-framework/watchdog) | Watchdog service — module health monitoring |

---

## 📄 License

This project is licensed under the **Apache 2.0** License — see the [LICENSE](LICENSE) file for details.

<div align="center">
  <sub>Built with ☕ in Java 21 · Designed for scale · Made at <a href="https://github.com/spool-framework">spool-framework</a></sub>
</div>
