# Getting Started

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step 1 — Clone the Repo](#step-1--clone-the-repo)
- [Step 2 — Set Up Python](#step-2--set-up-python)
- [Step 3 — Configure Your Environment](#step-3--configure-your-environment)
- [Step 4 — Provision Azure Infrastructure](#step-4--provision-azure-infrastructure)
- [Step 5 — Set Up Databricks Workspace](#step-5--set-up-databricks-workspace)
- [Step 6 — Upload Reference Data](#step-6--upload-reference-data)
- [Step 7 — Generate Test Data](#step-7--generate-test-data)
- [Step 8 — Deploy Pipelines](#step-8--deploy-pipelines)
- [Step 9 — Verify Everything Works](#step-9--verify-everything-works)

---

## Prerequisites

Before you start, you need:

1. **Azure subscription** — a personal pay-as-you-go account is fine (expect ~€10–20/month while learning)
2. **Databricks account** — create a free trial at [databricks.com](https://databricks.com) or use your Azure subscription to create a Databricks workspace
3. **Terraform installed** — [install guide](https://developer.hashicorp.com/terraform/install)
4. **Databricks CLI installed** — `pip install databricks-cli` or [install guide](https://docs.databricks.com/dev-tools/cli/index.html)
5. **Python 3.11+** — check with `python --version`
6. **pip** — for installing Python packages

---

## Step 1 — Clone the Repo

```bash
git clone https://github.com/ktnsh24/shopstream-databricks-data-platform.git
cd shopstream-databricks-data-platform
```

---

## Step 2 — Set Up Python

Install the required Python packages:

```bash
pip install loguru pyspark azure-eventhub
```

These are only needed for local development and running the data generators. The actual pipeline code runs inside Databricks — it has PySpark and all other libraries built-in.

---

## Step 3 — Configure Your Environment

Copy the example environment file and fill in your values:

```bash
cp .env.example .env
```

Open `.env` in a text editor and fill in:

| Variable | Where to find it |
|---|---|
| `AZURE_TENANT_ID` | Azure Portal → Azure Active Directory → Overview |
| `AZURE_SUBSCRIPTION_ID` | Azure Portal → Subscriptions |
| `ADLS_ACCOUNT_NAME` | You decide this — it must be globally unique (e.g. `yournamedataadls`) |
| `DATABRICKS_HOST` | Databricks workspace URL (shown after workspace is created in Step 5) |
| `DATABRICKS_TOKEN` | Databricks workspace → User Settings → Developer → Access Tokens → Generate |

Leave the Lakeflow Connect fields empty for now — you fill those in during the labs.

---

## Step 4 — Provision Azure Infrastructure

This creates your Azure storage account (ADLS Gen2), Event Hubs, and Key Vault:

```bash
cd terraform/azure
terraform init
terraform plan    # review what will be created
terraform apply   # type "yes" to confirm
```

**What gets created:**

- `helix-data-rg` resource group
- ADLS Gen2 storage account with containers: `raw/`, `checkpoints/`
- Azure Event Hubs namespace with a `orders-stream` topic
- Azure Key Vault for storing secrets

**Cost:** ~€2–5/month while active. Run `terraform destroy` when you are done with the project to avoid charges.

---

## Step 5 — Set Up Databricks Workspace

**Option A — Use your Azure subscription:**

1. In Azure Portal → search "Azure Databricks" → Create workspace
2. Choose your resource group (`helix-data-rg`), a name, and pricing tier (Standard is fine)
3. Wait ~5 minutes for deployment

**Option B — Databricks free trial:**

1. Go to [databricks.com](https://databricks.com) → Start Free Trial
2. Choose Azure as the cloud

**After your workspace is ready:**

1. Note the workspace URL (looks like `https://adb-1234567890.azuredatabricks.net`) — add it to `.env` as `DATABRICKS_HOST`
2. Create a personal access token: User Settings (top right) → Developer → Access Tokens → Generate → copy it to `.env` as `DATABRICKS_TOKEN`
3. Create a SQL Warehouse: SQL → SQL Warehouses → Create → choose Serverless → note the Warehouse ID

**Create Unity Catalog catalogs:**

Run this in a Databricks notebook (New → Notebook → SQL):

```sql
CREATE CATALOG IF NOT EXISTS helix_bronze;
CREATE CATALOG IF NOT EXISTS helix_silver;
CREATE CATALOG IF NOT EXISTS helix_gold;
```

---

## Step 6 — Upload Reference Data

The reference dimension tables (regions, product categories) are loaded from CSV files.
Upload them to your ADLS Gen2 storage:

Using Databricks UI:
1. Go to your workspace → Data → Add Data → ADLS Gen2
2. Upload `data/reference/regions.csv` to `raw/reference/regions.csv`
3. Upload `data/reference/product_categories.csv` to `raw/reference/product_categories.csv`

Or using Azure Storage Explorer (download from [storageexplorer.com](https://azure.microsoft.com/en-us/features/storage-explorer/)).

---

## Step 7 — Generate Test Data

Generate fake customer and returns data for the batch pipeline labs:

```bash
# Generate 1000 fake customers
python data_generators/generate_customers.py --rows 1000

# Generate 200 fake returns
python data_generators/generate_returns.py --rows 200
```

Files are written to `data_generators/output/`. Upload them to ADLS Gen2 the same way you uploaded the reference data:

- `customers_YYYYMMDD.csv` → `raw/customers/`
- `returns_YYYYMMDD.csv` → `raw/returns/`

---

## Step 8 — Deploy Pipelines

Deploy all pipelines and jobs to your Databricks workspace using Databricks Asset Bundles:

```bash
# Authenticate the CLI
databricks configure --token

# Deploy everything
databricks bundle deploy --target prod
```

This creates:
- Two Lakeflow Connect pipelines (customers + products from PostgreSQL)
- The streaming pipeline (Event Hubs → orders)
- The nightly batch pipeline (customers + products + returns → Silver → Gold)
- The delta maintenance job (weekly OPTIMIZE + VACUUM)

---

## Step 9 — Verify Everything Works

1. Go to Databricks workspace → Workflows → Pipelines
2. You should see `helix_streaming_pipeline` and `helix_nightly_batch_pipeline`
3. Start `helix_nightly_batch_pipeline` manually (click Start)
4. Watch the pipeline graph — each node turns green when it succeeds
5. When done, query a Gold table to verify data:

Open a notebook and run:

```sql
SELECT * FROM helix_gold.customers.fct_customer_metrics LIMIT 10;
```

If you see rows, your pipeline is working.

---

## Costs Reminder

Always clean up compute after a lab session. Databricks charges for clusters while they are running.

- Clusters auto-terminate after 30 minutes of inactivity (configured in Terraform)
- SQL Warehouses auto-stop after 10 minutes (configure in workspace UI)
- Streaming pipeline must be **manually stopped** — it runs forever if you leave it on

Run this to stop the streaming pipeline:

```bash
databricks pipelines stop <pipeline-id>
```

Or stop it from the workspace UI: Workflows → Pipelines → Stop.
