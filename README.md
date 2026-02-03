# AWS Honeypot → Microsoft Sentinel SIEM Pipeline

Defensive security lab project that deploys a **cloud honeypot** in AWS and routes telemetry into **Microsoft Sentinel (Log Analytics Workspace)** for correlation, querying (KQL), and visualization.

This repo focuses on **observability and detection engineering**: collecting signals, normalizing them, and building analysis that helps answer “what happened?” and “what should we improve?”

## Authorized use only

Run this lab only on infrastructure you own or have explicit written permission to test. This project is for controlled, defensive research.

## Goals

- Attract and observe common internet background noise (scanning, opportunistic probing, brute-force).
- Practice end-to-end telemetry handling (collection → transport → normalization → SIEM ingestion).
- Build repeatable analysis: top sources, time-series bursts, port distribution, and basic enrichment.

## Time window and metric definitions (study notes)

For the initial analysis, the observation window was **Jan 1 – Jan 9 (UTC)**.

**GeoIP caveat:** geolocation is context only and may be skewed by VPNs/proxies, carrier NAT, reassignment, or cloud egress.

**Network layer (VPC Flow Logs):**
- **Attempts** = count of ACCEPTed inbound flows to a destination port (scoped to the honeypot ENI/private IP).
- **ActiveMinutes** = number of distinct 1-minute bins with ≥1 such flow for a given source IP.

**Host layer (CloudWatch / sshd):**
- **Attempts** = count of SSH authentication failures (e.g., invalid user / failed password) for a given source IP.

## Architecture

See: `HoneyPot_SIEM.drawio`

High-level components (as implemented in the diagram):

- **AWS VPC** hosting the honeypot
- **EC2 honeypot instance** with “bait” services and multiple exposed TCP ports to simulate a misconfigured host
- **VPC Flow Logs** for network metadata (who/what port/when)
- **CloudWatch Logs** for host/service events (e.g., `sshd`)
- **GuardDuty** for additional threat signals/findings
- **Transport/storage**
  - **Kinesis Data Firehose** to deliver subscription-filtered logs to S3 (CloudWatch → Firehose path)
  - **S3 bucket** (archive/landing for delivered telemetry)
  - **S3 event notifications → SQS** for queue-driven ingestion workflows
- **Normalization**
  - **Lambda transform** (`lambda_function.py`) that converts CloudWatch subscription payloads into CSV-like rows for easier downstream parsing
- **Microsoft Sentinel / Log Analytics Workspace**
  - Azure data connectors (OIDC-based in the diagram) to ingest from AWS (S3/SQS paths)

## Exposed services (lab posture)

The honeypot’s internet-facing ports were selected to resemble a “mistakenly exposed” host and attract background scanning:

- **22** (SSH), **23** (Telnet), **80** (HTTP), **445** (SMB), **3389** (RDP), **1433** (MSSQL)

This is intentionally a controlled configuration for research in an isolated environment.

## Data flow (conceptual)

1. **Honeypot generates activity** (connection attempts, auth failures, etc.).
2. **AWS collects telemetry**
   - VPC Flow Logs record network-layer metadata.
   - CloudWatch collects host/service logs.
   - GuardDuty emits findings for relevant observed patterns.
3. **CloudWatch → Firehose → S3**
   - CloudWatch subscription filter streams logs to Firehose.
   - Firehose invokes the Lambda transform (`lambda_function.py`) to convert records to a simple CSV format.
   - Firehose delivers transformed records into S3.
4. **S3 → SQS → Sentinel ingestion**
   - S3 event notifications post to SQS.
   - Sentinel connector consumes events for ingestion into Log Analytics.
5. **KQL analytics & dashboards**
   - Queries aggregate patterns (top sources, bursts, persistence).
   - Workbooks visualize distributions (ports, geo, time series).

## Repo contents

- `HoneyPot_SIEM.drawio` — architecture diagram
- `HONEYPOT (1).pptx` — findings deck (initial analysis and lessons learned)
- `lambda_function.py` — Firehose transform for CloudWatch subscription payloads (gzip JSON → CSV rows)

## Findings summary (Jan 1–9 window)

- Ingestion volume was dominated by **VPC Flow Logs**, with surges around midnight to early morning UTC.
- Most inbound activity was **reconnaissance / opportunistic probing**, not targeted intrusion.
- Traffic concentration by exposed port (approx.):
  - **445 / 22 / 80 ≈ 94%** of events
  - **23 ≈ 5%**
  - **3389 + 1433 ≈ 1–2%** combined
- SSH host logs showed repeated auth failures focused on common/default usernames (e.g., `root`, `admin`, `ubuntu`, `debian`), consistent with commodity brute-force playbooks.
- No confirmed compromise indicators were observed in the available telemetry; conclusions are constrained by telemetry gaps and GeoIP limitations.

## Known issues / limitations (current state)

This is a learning lab and is not production-grade:

- **HTTP application logs were not ingested** during part of the run due to CloudWatch Agent/config instability; this reduced application-layer visibility (only flow-level metadata for HTTP).
- Cross-environment assumptions (regions, connector expectations, naming) may require adjustment.
- IAM scope is intentionally least-privilege oriented, but policy design is still evolving.

## IAM / JSON policy handling (public repo)

This repo intentionally does **not** include environment-specific JSON policies tied to real:
- AWS account IDs
- bucket/queue names
- ARNs
- KMS key identifiers
- Azure workspace identifiers/keys

If you later publish policy examples, prefer **templates** with placeholders (e.g., `${ACCOUNT_ID}`, `${REGION}`, `${BUCKET_NAME}`, `${QUEUE_NAME}`) and keep real policies private.

## Security guardrails for the lab

- Use a dedicated “lab” environment/account when possible.
- Apply restrictive security groups and limit egress if feasible.
- Set explicit retention/lifecycle rules for logs to control cost.
- Avoid collecting real credentials; any decoy login content should be non-production and not reused elsewhere.

## Future work

- Stabilize logging end-to-end (validate early, add health checks and alerting for silent failures).
- Expand normalization and schema versioning; improve correlation across Flow Logs, host logs, and GuardDuty findings.
- Add replay/backtesting from S3 to validate detections against stored telemetry.
- Improve documentation: assumptions, dependencies, and reproducible deployment steps.
- Explore safe AI assistance via MCP (e.g., guided triage checklists, summarization of findings, schema mapping suggestions), while keeping strict permission and safety boundaries.

## Disclaimer

This project is for defensive research and education. Use only in authorized environments and follow all applicable laws and policies.
