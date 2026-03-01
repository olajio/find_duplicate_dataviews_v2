# Find Duplicate Data Views

A Python script that scans multiple Elastic deployments to identify duplicate data views across all Kibana spaces. Duplicate data views (same title, different IDs) cause user confusion, break dashboard portability, and degrade cluster performance through redundant queries.

## Why This Matters

Duplicate data views are a common problem in Kibana. They typically appear when users manually create data views that already exist, or when Kibana objects are imported multiple times. Left unchecked, duplicates lead to:

- **Performance degradation** — dashboards referencing wildcard patterns like `*:filebeat-*` across duplicate views multiply search load on the cluster
- **User confusion** — team members see multiple identically-named data views and don't know which one to use, leading to inconsistent dashboards
- **Maintenance overhead** — updating or deprecating a data view pattern requires tracking down every copy across every space
- **SLA risk** — in highly regulated environments like ours, unnecessary cluster load from duplicates can push response times past SLA thresholds

This script gives you a single-command way to audit all of this across 100+ deployments.

## What It Does

For each configured cluster, the script:

1. Connects to the Kibana API
2. Discovers all Kibana spaces automatically
3. Retrieves all data views in each space
4. Groups data views by title and flags duplicates (same title, multiple IDs)
5. Counts how many saved objects (dashboards, visualizations, lenses, etc.) reference each duplicate
6. Outputs a consolidated report showing deployment, space, duplicate titles, IDs, and reference counts

The reference count is critical — it tells you which duplicate is actively used and which is safe to remove.

## Prerequisites

- Python 3.8+
- `requests` library (`pip install requests`)
- Kibana API keys with read access to spaces, data views, and saved objects for each deployment

## Quick Start

### 1. Create your config file

Create a `clusters.json` file with your deployment details:

```json
{
  "clusters": {
    "prod": {
      "kibana_url": "https://prod-kibana:5601",
      "api_key": "$PROD_KIBANA_API_KEY",
      "verify_ssl": false
    },
    "qa": {
      "kibana_url": "https://qa-kibana:5601",
      "api_key": "$QA_KIBANA_API_KEY",
      "verify_ssl": false
    }
  }
}
```

API keys can be specified directly or as environment variable references (prefixed with `$`).

### 2. Set your API keys

```bash
export PROD_KIBANA_API_KEY="your-prod-api-key-here"
export QA_KIBANA_API_KEY="your-qa-api-key-here"
```

Or store them in a `.env` file and source it:

```bash
source .env
```

### 3. Test connectivity

```bash
python find_duplicate_dataviews.py --config clusters.json --connectivity-check
```

Expected output:

```
🔌 CONNECTIVITY CHECK
============================================================
  ✅ prod                 — Connected (12 spaces)
  ✅ qa                   — Connected (8 spaces)

  Result: 2/2 clusters reachable
```

### 4. Run the scan

```bash
# Scan all clusters
python find_duplicate_dataviews.py --config clusters.json

# Scan specific clusters only
python find_duplicate_dataviews.py --config clusters.json --clusters prod qa

# Scan with concurrent workers (recommended for 10+ clusters)
python find_duplicate_dataviews.py --config clusters.json --workers 10

# Export results to CSV
python find_duplicate_dataviews.py --config clusters.json --output csv

# Export results to JSON
python find_duplicate_dataviews.py --config clusters.json --output json
```

## Sample Output

```
==========================================================================================
DUPLICATE DATA VIEWS REPORT
==========================================================================================

──────────────────────────────────────────────────────────────────────────────────────────
📦 DEPLOYMENT: PROD
──────────────────────────────────────────────────────────────────────────────────────────

  🔹 Space: Team Alpha

    Data View Title: filebeat-*
    Copies: 3
      ID: a1b2c3d4-e5f6-7890-abcd-ef1234567890  (47 references)
      ID: f9e8d7c6-b5a4-3210-fedc-ba0987654321  (2 references)
      ID: 11223344-5566-7788-99aa-bbccddeeff00  (0 references)

  🔹 Space: Team Beta

    Data View Title: metricbeat-*
    Copies: 2
      ID: aabbccdd-eeff-0011-2233-445566778899  (31 references)
      ID: 99887766-5544-3322-1100-ffeeddccbbaa  (0 references)

==========================================================================================
SUMMARY
==========================================================================================
  Deployments with duplicates : 1
  Duplicate title groups       : 2
  Total duplicate data view IDs: 5
==========================================================================================
```

## Command Reference

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to clusters JSON config file | Required |
| `--clusters` | Space-separated list of cluster names to scan | All clusters |
| `--output` | Output format: `table`, `csv`, or `json` | `table` |
| `--output-file` | Custom output file path for csv/json exports | Auto-generated with timestamp |
| `--connectivity-check` | Test connectivity to all clusters and exit | Off |
| `--workers` | Number of concurrent workers for parallel scanning | `1` |
| `--verbose` | Enable debug-level logging | Off |

## Config File Structure

Each cluster entry requires only three fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kibana_url` | string | Yes | Kibana endpoint URL |
| `api_key` | string | Yes | Kibana API key (direct value or `$ENV_VAR` reference) |
| `verify_ssl` | boolean | No | Whether to verify SSL certificates (default: `true`) |

## Managing 100+ Deployments

For large environments, recommended practices:

- **Use environment variables for API keys** — keep secrets out of the config file so it can be version-controlled safely
- **Use a `.env` file** — store all `export KEY=value` lines in one file and `source .env` before running
- **Use `--workers 10`** (or higher) — concurrent scanning dramatically reduces total runtime across many clusters
- **Use `--clusters`** to target specific deployments when troubleshooting rather than scanning everything
- **Schedule regular scans** — run via cron or CI/CD to catch new duplicates early
- **Export to CSV** — track duplicate counts over time or share reports with team leads

## Interpreting Results

When reviewing duplicates, the **reference count** tells you what to do:

- **High reference count** → This is the "real" data view that dashboards and visualizations depend on. Keep it.
- **Zero references** → This is an orphaned duplicate. Safe to delete.
- **Low references** → Investigate which objects reference it. Consider migrating those objects to the primary data view, then delete.

## Troubleshooting

**"Environment variable not set. Skipping cluster."**
The API key references an env var (e.g., `$PROD_KIBANA_API_KEY`) that isn't exported in your shell. Run `export PROD_KIBANA_API_KEY=your-key` or check your `.env` file.

**"Failed to retrieve spaces"**
The API key may lack permissions, the Kibana URL may be wrong, or the cluster may be unreachable. Run `--connectivity-check` to diagnose.

**Timeouts on large clusters**
The script uses a 30-second timeout for space/data-view calls and 60 seconds for object reference scans. If you have spaces with very large numbers of saved objects, consider running with fewer concurrent workers to reduce load.
