<div align="center">

<img src="assets/spool_icon.svg" alt="Spool Logo" width="220"/>

<br/>

# SPOOL

### *An agnostic, auditable & event-driven data ingestion framework*

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Build](https://img.shields.io/badge/build-Maven-red?logo=apachemaven)](https://maven.apache.org/)
[![Organization](https://img.shields.io/badge/GitHub-spool--framework-181717?logo=github&logoColor=white&style=flat)](https://github.com/spool-framework)

<br/>

> **[github.com/spool-framework](https://github.com/spool-framework)**

</div>

---

> **SPOOL** is a Java-based, technology-agnostic framework for building scalable, auditable data ingestion pipelines.  
> It follows an **event-driven** and **ports & adapters** architecture, exposing clean extension points via SPI so you can plug in any technology (Kafka, S3, PostgreSQL, local filesystem) without touching core logic.

---

## 📑 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Quickstart](#-quickstart)
- [Modules](#-modules)
  - [Runtime](#-runtime)
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
- **Observable** — built-in OpenTelemetry integration (traces, metrics, logs) and health check endpoints per module, compatible with ECS and Kubernetes probes
- **Resilient** — built-in circuit breaker (sliding-window) and retry policies protect every inter-module boundary

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
                                    ▲
                             ┌─────────────┐
                             │   Runtime   │
                             │ (bootstrap) │
                             └─────────────┘
```

All inter-module communication is **event-driven**. The infrastructure (broker, inbox, data lake) is declared once in a shared YAML descriptor and injected into every module via the `infrastructure` SPI.

---

## 🚀 Quickstart

The fastest way to run a SPOOL pipeline is via the **Runtime** module and a single YAML descriptor.

**1. Add the dependency**

```xml
<dependency>
    <groupId>io.github.spool-framework</groupId>
    <artifactId>runtime</artifactId>
    <version>1.1.0</version>
</dependency>
```

**2. Create your descriptor** (`src/main/resources/spool.yaml`)

```yaml
infrastructure:
  watchdog: "http://localhost:8090"

  eventBus:
    type: IN_MEMORY

  inbox:
    type: FILE_SYSTEM
    configuration:
      path: "/var/spool/inbox"

  dataLake:
    type: FILE_SYSTEM
    configuration:
      path: "/var/spool/datalake"

modules:
  - crawler:
      type: POLL
      id: my-crawler
      source:
        type: HTTP
        configuration:
          sourceId: my-api
          url: "https://api.example.com/data"
          scheduleMilliseconds: 10000
        mediaType: JSON_ARRAY

  - ingester:
      id: my-ingester
      type: REACTIVE

  - janitor:
      id: my-janitor
      configuration:
        milliseconds: 5000
        millisecondsThreshold: 30000
        millisecondsTTL: 300000
```

**3. Boot the runtime**

```java
public class Main {
    public static void main(String[] args) {
        SpoolRuntime.builder()
            .withNodeFromDSL("/spool.yaml")
            .build()
            .start();
    }
}
```

That's it — SPOOL reads your descriptor, wires every module, and starts the pipeline. Swap `FILE_SYSTEM` for `S3` or `KAFKA` in the YAML without changing a line of Java.

---

## 📦 Modules

### ⚡ Runtime

> Bootstraps and starts a full SPOOL pipeline from code or a YAML descriptor.

`runtime` is the entry point of any SPOOL application. `SpoolRuntimeBuilder` assembles one or more `SpoolNode`s — either loaded from YAML descriptors via `withNodeFromDSL(path)` or wired programmatically via `withNode(node)` — and hands them to `SpoolRuntime`, which starts them all.

- Fluent builder API — mix DSL-driven and programmatic nodes in the same runtime
- Optional OpenTelemetry bootstrap — pass an `OpenTelemetryConfiguration` to enable distributed tracing, metrics and logs from startup
- Zero framework coupling — plain Java, no reflection magic

---

### 🕷 Crawler

> Acquires raw data from external sources on a configurable schedule.

The Crawler polls or streams data from any source and emits raw records into the **inbox**.  
It uses a pipeline of `Normalizer → Splitter → Serializer` to normalize heterogeneous payloads (JSON objects, JSON arrays, nested paths, images, PDFs) into discrete events before publishing.

Three source strategies are supported:

| Strategy | Description |
|---|---|
| `POLL` | Pulls data on a fixed schedule from any `PollSource` (HTTP, SQL, ...) |
| `STREAM` | Consumes from a push-based `StreamSource` (in-memory, custom) |
| `WEBHOOK` | Exposes HTTP routes and reacts to incoming push events |

- Configurable poll schedule in milliseconds
- Supports `JSON_OBJECT`, `JSON_ARRAY` and path-based `rootPath` extraction
- Image and PDF normalizers included out of the box
- Fully decoupled from any serialization library via SPI factories
- Idempotency check before insertion using `sourceId + payload`

---

### ✅ Ingester

> Validates and persists incoming raw events to the Data Lake.

The Ingester consumes events from the inbox, runs them through a **chain of validators** discovered via SPI, and writes valid records to the raw Data Lake layer.  
Invalid events are quarantined rather than discarded, preserving full auditability.

- Reactive ingestion mode with configurable buffer and flush policy
- Validator chain ordered by priority
- Validators return pass/fail results — no exception-driven control flow
- Constructs S3/filesystem partition paths from the `PartitionKeySchema` embedded in each event

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
- FileSystem and raw filesystem datamart writers included

---

### 🧹 Janitor

> Keeps the inbox clean and ensures no envelope is silently lost.

The Janitor is a maintenance module that runs autonomously alongside the pipeline.  
It removes envelopes that have already been successfully persisted and republishes those that have been stuck for too long, acting as a self-healing mechanism against processing failures.

- Purges persisted envelopes from the inbox
- Detects and republishes stale/stuck envelopes
- Configurable staleness threshold and TTL (in milliseconds)
- Complements the quarantine strategy of the Ingester

---

### 🐕 Watchdog

> Monitors the health of all running SPOOL modules.

The Watchdog is a **standalone service** that receives heartbeats from every module and exposes a unified health endpoint.  
It detects failures via missed heartbeat thresholds and can be deployed as a self-contained Docker image, ready for ECS or Kubernetes.

- REST endpoints for module registration, heartbeat and health query
- OpenTelemetry-based module observer for metrics and alerting
- In-memory module registry with automatic state transitions
- Compatible with ECS health checks and Kubernetes liveness probes
- Embeds a lightweight HTTP server on a configurable port

---

### 🧱 Core

> Shared contracts, domain models and common utilities.

`core` is the foundation of the entire framework. It hosts the domain model, shared interfaces and utility classes used across all other modules.  
No module depends on another module's internals — they all depend on `core`.

Highlights:
- **Resilience** — built-in circuit breaker (count/time sliding window) and `RetryingExecutor` with configurable policies
- **Observability** — OpenTelemetry traces, metrics and logs wired to every event bus and inbox operation
- **Pipeline API** — composable `Step<I,O>` and `Pipeline<I,O>` with metered and observed decorators
- **Health** — `HealthProbe`, `HealthServer` and `PolledHealthProbe` for any module
- **Routing** — `EventRouter`, `ChannelRouter` and `ErrorRouter` for flexible event dispatch

---

### ⚙️ Infrastructure

> SPI and base providers for pluggable infrastructure.

`infrastructure` defines the SPI contracts and ships concrete adapters for the most common technologies.  
Annotate your own provider class with `@SpoolPlugin` and it will be discovered automatically at runtime — no manual registration.

**Included adapters**

| Concern | Implementations |
|---|---|
| Event Bus | Kafka, In-Memory |
| Inbox | File System, S3 |
| Data Lake | File System, S3 |
| Datamart | File System, Raw File System |
| Poll Source | HTTP, SQL |
| Stream Source | In-Memory |
| Normalizers | JSON Object, JSON Array, Image, PDF, EventClass, EventClass Array |

---

### 📝 DSL

> Declarative YAML descriptor for module configuration.

The `dsl` module defines and validates the YAML configuration language used to describe any SPOOL node.  
It provides the parser, the configuration model, and the constraint validation so that misconfigured descriptors fail fast with clear error messages.

`SpoolNodeDSL.fromDescriptor(path)` reads a YAML file from the classpath and returns a fully wired `SpoolNode` ready to be handed to `SpoolRuntime`.

---

## 🌊 Data Flow

```
External Source
      │
      ▼
  [Crawler]  ←── POLL / STREAM / WEBHOOK
      │  raw records
      ▼
  [Raw Inbox]  ←── idempotency check       ◀── [Janitor] purges & republishes
      │
      ▼
  [Ingester]  ←── validator chain (SPI) · buffer & flush
      │  valid events / quarantine
      ▼
  [Raw Data Lake] ←── Medallion Architecture (S3 / FileSystem)
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
Infrastructure is declared once and shared across all modules in the node.

**Full example with Kafka + S3**

```yaml
infrastructure:
  watchdog: "http://localhost:8090"

  eventBus:
    type: KAFKA
    configuration:
      bootstrapServers: "localhost:9092"

  inbox:
    type: S3
    configuration:
      bucket: "spool-inbox"
      region: "eu-west-1"

  dataLake:
    type: S3
    configuration:
      bucket: "spool-datalake"
      region: "eu-west-1"

modules:
  - crawler:
      type: POLL
      id: market-crawler
      source:
        type: HTTP
        configuration:
          sourceId: market-api
          url: "https://api.example.com/v1/trades/BTCUSD?limit=100"
          scheduleMilliseconds: 60000
        mediaType: JSON_ARRAY
      eventMapping:
        namingConvention: SNAKE_CASE

  - ingester:
      id: market-ingester
      type: REACTIVE

  - janitor:
      id: market-janitor
      configuration:
        milliseconds: 5000
        millisecondsThreshold: 30000
        millisecondsTTL: 300000
```

**Minimal local example (File System)**

```yaml
infrastructure:
  watchdog: "http://localhost:8090"
  eventBus:
    type: IN_MEMORY
  inbox:
    type: FILE_SYSTEM
    configuration:
      path: "/var/spool/inbox"
  dataLake:
    type: FILE_SYSTEM
    configuration:
      path: "/var/spool/datalake"

modules:
  - crawler:
      type: POLL
      id: my-crawler
      source:
        type: HTTP
        configuration:
          sourceId: my-api
          url: "https://api.example.com/data"
          scheduleMilliseconds: 10000
        mediaType: JSON_OBJECT
  - ingester:
      id: my-ingester
      type: REACTIVE
  - janitor:
      id: my-janitor
      configuration:
        milliseconds: 5000
        millisecondsThreshold: 30000
        millisecondsTTL: 300000
```

---

## 🏢 Organization

<div align="center">

### **[github.com/spool-framework](https://github.com/spool-framework)**

*All repositories live under the `spool-framework` GitHub organization.*

</div>

| Repository | Description |
|---|---|
| [`core`](https://github.com/spool-framework/core) | Shared contracts, domain models, resilience and common utilities |
| [`infrastructure`](https://github.com/spool-framework/infrastructure) | SPI, annotation and base providers for pluggable infrastructure |
| [`dsl`](https://github.com/spool-framework/dsl) | Declarative YAML configuration language and parser |
| [`runtime`](https://github.com/spool-framework/runtime) | Bootstrap layer — starts a full pipeline from a YAML descriptor |
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
