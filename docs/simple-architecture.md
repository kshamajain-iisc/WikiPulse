# WikiPulse Simple Architecture

```mermaid
flowchart LR
    A[Wikimedia Data<br/>Live events, historical data,<br/>revision and page information]
    B[Ingestion and Processing<br/>Kafka, Spark Streaming,<br/>batch processing and ML]
    C[Shared Data Layer<br/>Raw, cleaned and result data<br/>with fast dashboard access]
    D[WikiPulse Dashboards<br/>Live alerts, history, edit risk,<br/>churn, bots, graph and map]

    A --> B --> C --> D
```

Wikimedia live and historical data enters one processing layer, is stored in a shared data layer, and is presented through the WikiPulse dashboards.
