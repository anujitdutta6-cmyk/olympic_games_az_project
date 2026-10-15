# Olympic Games Data Engineering on Azure

An Azure data engineering portfolio project for organizing Olympic Games datasets with **Azure Data Factory (ADF)**, **Azure Data Lake Storage Gen2 (ADLS Gen2)**, and **Azure Databricks / Apache Spark**. The repository contains notebooks for parameter handling and entity-level processing, including athletes, coaches, events, NOCs, and curated entities.

> **Scope note:** This README documents the workflow suggested by the repository artifacts and established Azure lakehouse patterns. Notebook source alone cannot confirm live ADF activity settings, storage paths, schedules, credentials, permissions, or deployed resources. Verify those items in the Azure portal before describing them as deployed or production-ready.

## Contents

- [Project objectives](#project-objectives)
- [Architecture](#architecture)
- [End-to-end data flow](#end-to-end-data-flow)
- [Repository walkthrough](#repository-walkthrough)
- [Azure services and responsibilities](#azure-services-and-responsibilities)
- [Data quality and reliability](#data-quality-and-reliability)
- [Security and governance](#security-and-governance)
- [Operations and troubleshooting](#operations-and-troubleshooting)
- [Runbook](#runbook)
- [Production-readiness roadmap](#production-readiness-roadmap)
- [Real-world scenarios](#real-world-scenarios)
- [Interview preparation](#interview-preparation)
- [References](#references)

## Project objectives

1. Organize Olympic datasets into entities such as athletes, coaches, events, and National Olympic Committees (NOCs).
2. Use ADF to orchestrate ingestion and processing.
3. Use ADLS Gen2 as cloud storage for source and processed data.
4. Transform and curate datasets with Databricks notebooks and Spark.
5. Separate workflow orchestration from distributed processing.
6. Establish a foundation for repeatable, observable, maintainable data pipelines.

## Architecture

### Logical architecture

**Olympic source datasets → Azure Data Factory → ADLS Gen2 landing/raw storage → Azure Databricks → entity processing and validation → curated/Silver datasets → downstream analytics**

- **Sources:** Olympic datasets such as athletes, coaches, events, and NOC reference data.
- **ADF:** Coordinates ingestion and dependencies. Depending on the deployed implementation, this may involve Copy activities, parameters, datasets, linked services, and Databricks activities.
- **ADLS Gen2:** Stores source and processed files durably.
- **Databricks:** Runs Spark-based transformations in notebooks.
- **Curated datasets:** Provide a cleaner, more consistent foundation for analysis and downstream consumption.

This is a logical architecture, not a claim that every component is already deployed or connected. Confirm actual pipeline activities, storage paths, and output formats in your workspaces.

### Medallion architecture interpretation

| Layer | Responsibility | Typical contents |
|---|---|---|
| **Bronze / Raw** | Preserve source data with minimal changes | Original Olympic files and ingestion metadata |
| **Silver / Curated** | Clean, validate, standardize, and relate entities | Curated athletes, coaches, events, and NOC datasets |
| **Gold / Analytics-ready** | Model business metrics and aggregates | Optional medal summaries, country performance, and event analytics |

The repository artifacts indicate entity-level and curated processing. Do not claim a Gold layer is implemented unless you can point to its notebook, table, storage path, or pipeline. Gold is a recommended extension if it is not currently present.

## End-to-end data flow

1. **Ingestion orchestration — ADF:** Configure source and sink connections, datasets, parameters, and activities to move Olympic data into the lake. ADF coordinates the workflow; it does not replace Spark's distributed transformation engine.
2. **Landing / raw storage — ADLS Gen2:** Store ingested source data in a clearly named landing or raw location. Preserve original inputs where possible so that transformations can be rerun and data issues investigated.
3. **Processing — Databricks notebooks:** Run notebooks for parameter handling and entity processing. Use reusable logic and parameterization where supported by the current implementation.
4. **Entity curation — Silver:** Apply notebook transformations to produce curated entity datasets. Validate schema, required fields, duplicates, and relationships before treating outputs as trusted.
5. **Consumption:** Use curated outputs for SQL analysis, dashboards, or a future Gold layer. Document a consumer only when it is actually configured.
6. **Observability:** Track each pipeline run, notebook execution, input/output location, row counts, and failures. Where these controls are not implemented, treat them as production-readiness work.

## Repository walkthrough

The repository includes notebook artifacts with names corresponding to these areas:

| Notebook | Purpose suggested by its name |
|---|---|
| Array_Parameter.ipynb | Array or parameter handling |
| Curated_Entities.ipynb | Curated entity processing |
| Silver_Entities_Coaches&events.ipynb | Coaches and events entity processing |
| Silver_Entities_NOCS.ipynb | NOC entity processing |
| Silver_Entity_Atheletes.ipynb | Athlete entity processing |

Notebook filenames are shown as they appear in the repository; spelling and naming can be standardized later. Confirm exact transformations, source paths, schemas, and write modes by reviewing each notebook's code and associated ADF activities.

## Azure services and responsibilities

| Component | Responsibility | What to verify |
|---|---|---|
| Azure Data Factory | Orchestration and dependency management | Triggers, activities, parameters, linked services, datasets, retry policy |
| ADLS Gen2 | Durable data storage | Filesystem/container names, folder conventions, hierarchical namespace, lifecycle |
| Azure Databricks | Spark processing and notebook execution | Workspace, compute policy, notebook paths, job configuration, runtime |
| Microsoft Entra ID / managed identity | Identity used by Azure services | Actual identity and its role assignments |
| Unity Catalog, if configured | Governance and access to tables/locations | Catalog, schema, grants, external locations, storage credentials |

Only describe a component as implemented if it exists in your environment. Unity Catalog, Key Vault, CI/CD, alerts, and a Gold layer are recommendations unless confirmed in the deployed solution.

## Data quality and reliability

For a dependable pipeline, validate the following at appropriate boundaries:

- **Schema:** Required columns exist and data types are compatible.
- **Required values:** Keys and essential business fields are not unexpectedly null.
- **Uniqueness:** Entity keys do not contain unintended duplicates.
- **Referential integrity:** Relationships between entities are valid where applicable.
- **Record reconciliation:** Compare input, accepted, rejected, and output row counts.
- **Invalid data:** Quarantine malformed records with a reason rather than silently losing them.
- **Repeatability:** Reruns should not create unintended duplicate records.
- **Schema changes:** Detect unexpected source-column or data-type changes and decide whether to accept, reject, or quarantine them.

These are recommended controls. Clearly identify which checks are already implemented in code.

## Security and governance

- Use managed identities or service principals where supported, with least-privilege access.
- Grant storage access to the identity actually used by the pipeline or Databricks compute. Do not assume the workspace identity is automatically used for every operation.
- Keep credentials, tokens, connection strings, and secrets out of notebooks and Git history. Use Key Vault or an approved secret-management mechanism where appropriate.
- Use Unity Catalog permissions and governed storage locations if Unity Catalog is part of the workspace design.
- Separate development and production resources and permissions for a production implementation.
- Do not commit private data, subscription identifiers, account keys, or exported credentials.

## Operations and troubleshooting

| Symptom | Likely area to inspect | Practical check |
|---|---|---|
| ADF activity cannot connect | Linked service, integration runtime, network, identity | Inspect the activity error and validate connectivity and authentication |
| Databricks cannot read a storage path | Path, storage permissions, credential configuration | Confirm the ABFSS path, filesystem/container, and identity's data-plane permissions |
| Access denied while writing | Storage role assignment or ACL | Check required role assignments and directory/file ACLs for the actual identity |
| Spark schema/type mismatch | Source schema drift or incompatible file types | Inspect the failing column's physical type and Spark schema; apply explicit, justified casting |
| Duplicate records after rerun | Append/write strategy or missing key logic | Define a stable business key and select a safe overwrite, deduplication, or merge strategy |
| Pipeline succeeds but output is missing | Sink path, conditional branch, notebook behavior | Verify activity inputs/outputs and confirm the expected target location |
| Unexpectedly high cloud cost | Compute uptime, job triggers, networking, storage | Inspect Cost Analysis, schedules, compute configuration, and idle resources |

Start with the exact failing activity or notebook error. Avoid broad permission changes or deleting resources as a first troubleshooting step.

## Runbook

Adapt this runbook to the actual resource names and pipeline design.

### Prerequisites

- Azure subscription permissions for the relevant resource group.
- Access to Azure Data Factory, ADLS Gen2, and Azure Databricks.
- Required source data and permission to read it.
- Correct identity and permissions for ADF and Databricks to access storage.

### Execute and validate

1. Review the ADF pipeline and confirm source, sink, parameters, and dependencies.
2. Confirm the expected source files exist in the configured location.
3. Start a debug or manual pipeline run in a non-production environment.
4. Review every activity's status and input/output details.
5. Confirm the Databricks notebook ran successfully and wrote to the expected location.
6. Validate output schema, row counts, key uniqueness, and sample records.
7. Confirm a second run behaves as intended and does not introduce duplicate data.
8. Record the outcome, rejected records, warnings, and follow-up actions.

### Deployment guidance

For a team or production workflow, keep notebooks and pipeline definitions in version control, separate environments, deploy through a reviewed release process, and test changes against a small representative dataset before a full run.

## Production-readiness roadmap

The following items are suggested improvements, not assertions about the current implementation.

- [ ] Document exact ADF pipeline names, activity dependencies, triggers, and parameters.
- [ ] Document storage filesystem/container and folder paths for each layer.
- [ ] Add schema, null, uniqueness, and referential-integrity checks.
- [ ] Add run-level audit metadata: run ID, start/end time, status, row counts, and error details.
- [ ] Define retry, timeout, failure handling, and alerting rules.
- [ ] Make reruns safe and document the incremental/full-load strategy.
- [ ] Store secrets securely and apply least-privilege access.
- [ ] Add automated tests for transformation rules and representative data.
- [ ] Establish dev/test/prod configuration and CI/CD.
- [ ] Review compute sizing, auto-termination, scheduling, and cloud costs.
- [ ] Add a Gold/analytics layer only if there is a clear downstream reporting requirement.

## Real-world scenarios

### Scenario 1 — A source file arrives with new columns

**Risk:** A transformation fails or silently drops new information.

**Approach:** Detect schema drift, preserve the raw source, validate the changed schema, and explicitly decide whether to accept or quarantine new fields. Avoid blindly casting every column.

### Scenario 2 — The same data is processed twice

**Risk:** Duplicate records or inflated business metrics.

**Approach:** Define the business key and load semantics. Use deterministic deduplication and an idempotent write pattern. Use Delta Lake MERGE only when the target model and key logic support it.

### Scenario 3 — Athlete and event datasets disagree

**Risk:** Invalid joins and misleading reports.

**Approach:** Validate join keys, check unmatched records, measure join cardinality, and decide whether unmatched records should be quarantined or retained with an explicit status.

### Scenario 4 — A pipeline fails after raw ingestion

**Risk:** A rerun starts from the wrong point or repeats work.

**Approach:** Keep stages independently observable, preserve raw inputs, and make downstream writes rerunnable. Use ADF dependency conditions and retry settings deliberately.

### Scenario 5 — The pipeline must support larger datasets

**Risk:** Long runtimes, memory pressure, and unnecessary compute cost.

**Approach:** Inspect Spark execution plans and data skew, avoid unnecessary collect operations, select only needed columns, filter early when correct, and tune partitioning based on measured data size rather than arbitrary partition counts.

## Interview preparation

Use these answers as a framework and adapt them to what you actually implemented.

**Q: Why use ADF and Databricks together?**  
ADF coordinates ingestion, dependencies, scheduling, and monitoring. Databricks provides distributed Spark compute for transformations. This separates orchestration from data processing.

**Q: What is the role of ADLS Gen2?**  
It is the durable storage layer for source and processed data. A well-organized lake supports independent compute and reprocessing when raw data is retained.

**Q: Explain Bronze, Silver, and Gold.**  
Bronze preserves source data, Silver applies quality and consistency rules, and Gold presents business-oriented models or aggregates. The layers are logical quality boundaries; their physical implementation depends on the project.

**Q: How do you make a pipeline idempotent?**  
Define the key and load semantics, make writes deterministic, and ensure a retry or rerun does not duplicate data or corrupt existing results. The implementation might use overwrite, deduplication, checkpoints, or MERGE, depending on the source and target.

**Q: How do you handle schema evolution?**  
Detect changes, preserve the source, validate compatibility, and explicitly accept, transform, or quarantine changes. Schema evolution should be governed rather than enabled without review.

**Q: How do you troubleshoot a storage permission error?**  
Identify the exact identity used by the failing operation, verify the storage path and network route, then check the required data-plane role assignment and ACLs. Workspace access alone does not guarantee file access.

**Q: How do you measure data quality?**  
Use measurable rules such as required-field completeness, uniqueness, accepted-value constraints, referential integrity, and source-to-target reconciliation. Track rejected records and quality trends over time.

**Q: How would you productionize this project?**  
Add configuration by environment, managed identity and least privilege, monitoring and alerts, data-quality gates, repeatable deployments, automated tests, documented recovery steps, and cost controls.

**Interview honesty tip:** Separate what is implemented in this repository from what you would add in production. Be ready to point to the notebook, ADF activity, storage location, or configuration that supports each claim.

## References

- [Azure Data Factory — overview and concepts](https://learn.microsoft.com/en-us/azure/data-factory/introduction)
- [Azure Architecture Center — design a medallion lakehouse with Azure Data Factory](https://learn.microsoft.com/en-us/azure/architecture/databases/architecture/azure-data-factory-on-azure-landing-zones-index)
- [Azure Databricks — medallion lakehouse architecture](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion)
- [Azure Databricks — architecture overview](https://learn.microsoft.com/en-us/azure/databricks/getting-started/architecture)
- [Tutorial video reference](https://youtu.be/ESWqAZP2qA4)

---

**Project status:** Portfolio / learning project. Validate live Azure configuration and mark optional production controls as implemented only after confirming them in the workspace.
