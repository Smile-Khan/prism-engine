# 💎 Prism Discovery Engine

**Prism** is an industrial-grade, high-throughput distributed search and discovery engine built on the **CQRS (Command Query Responsibility Segregation)** pattern. It leverages modern Java 21 capabilities to provide real-time data synchronization between a relational source of truth and a full-text search Read-Model.

---

## 🏗️ System Architecture

The project implements a resilient event-driven pipeline:
1. **Command Side (Write-Model):** A Spring Boot service managing event ingestion into **PostgreSQL**.
2. **CDC Bridge:** **Debezium** monitors Postgres WAL (Write-Ahead Logs) and streams changes to **Kafka**.
3. **Query Side (Read-Model):** A high-concurrency service consuming Kafka signals to project data into **Elasticsearch**.

![Architecture Flow](https://img.shields.io/badge/Architecture-CQRS%20%7C%20CDC%20%7C%20Event--Driven-blue)
![Java Version](https://img.shields.io/badge/Java-21-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.2-green)
![Kafka](https://img.shields.io/badge/Backbone-Apache%20Kafka-black)

## 🚀 Key Features

- **Java 21 Virtual Threads (Project Loom):** Optimized for high-throughput I/O between Kafka and Elasticsearch without thread starvation.
- **Change Data Capture (CDC):** Real-time synchronization using Debezium, ensuring the Read-Model is eventually consistent with the Source of Truth.
- **Advanced Discovery:**
  - **Fuzzy Search:** Typo-tolerant querying using Elasticsearch Native Query DSL.
  - **Hit Highlighting:** Automatic HTML match indication (`<em>` tags) in search results for enhanced UX.
- **Production Resilience:**
  - **Non-blocking Retries:** Exponential backoff strategy for transient failures.
  - **Dead Letter Topics (DLT):** Fault-tolerant event handling to prevent data loss.
- **Industrial Standards:** Multi-module Maven architecture, strictly typed DTOs using Java Records, and conventional git-flow.

## 🛠️ Tech Stack

- **Runtime:** Java 21 (Virtual Threads enabled)
- **Framework:** Spring Boot 3.4.2
- **Database:** PostgreSQL 16 (Logical Replication)
- **Messaging:** Apache Kafka
- **Search Engine:** Elasticsearch 8.11
- **Ingestion:** Debezium Connect
- **Build Tool:** Maven (Multi-Module)

## 📁 Project Structure


prism/
├── prism-command-service/   # Write-Model API (Ingestion -> Postgres)
├── prism-search-service/    # Read-Model API (Kafka -> Elasticsearch -> Search API)
├── docker-compose.yml       # Infrastructure orchestration
└── pom.xml                  # Root Aggregator & Dependency Management


🚦 Getting Started
Prerequisites
Docker & Docker Compose
Java 21 SDK
Maven 3.9+
Infrastructure Setup
Spin up the distributed backbone:
docker-compose up -d


Build & Run
From the root directory:

mvn clean install -DskipTests
# Start Command Service
cd prism-command-service && ./mvnw spring-boot:run
# Start Search Service
cd prism-search-service && ./mvnw spring-boot:run

🔍 API Highlights
Ingest an Event (Command)
POST http://localhost:8080/api/v1/events

JSON
{
    "title": "Smile Khan Flagship Project",
    "category": "ENGINEERING",
    "metadata": { "status": "active" }
}

Discover Results (Query)
POST http://localhost:8081/api/v1/discovery/search

JSON
{
    "query": "Flagship",
    "category": "ENGINEERING",
    "page": 0,
    "size": 10
}

Author: Smile-Khan
Role: Java Backend Specialist | Distributed Systems Architect
