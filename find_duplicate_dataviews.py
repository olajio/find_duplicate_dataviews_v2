#!/usr/bin/env python3
"""
Find Duplicate Data Views Across Multiple Kibana Deployments

This script loops through multiple Elasticsearch/Kibana deployments (read from a
JSON config file), scans all spaces in each deployment, and identifies duplicate
data views (same title appearing multiple times within a space). For each
duplicate it reports:
  - Deployment name
  - Kibana space
  - Data view title and IDs
  - Number of saved-object references per data view ID

Usage:
    # Scan ALL clusters defined in config
    python find_duplicate_dataviews.py --config clusters.json

    # Scan specific clusters only
    python find_duplicate_dataviews.py --config clusters.json --clusters prod qa

    # Test connectivity without scanning
    python find_duplicate_dataviews.py --config clusters.json --connectivity-check

    # Export results to CSV
    python find_duplicate_dataviews.py --config clusters.json --output csv

    # Run with concurrent workers for speed
    python find_duplicate_dataviews.py --config clusters.json --workers 5

Config file format (clusters.json):
{
  "clusters": {
    "prod": {
      "kibana_url": "https://prod-kibana:5601",
      "api_key": "YOUR_API_KEY_HERE",
      "verify_ssl": false,
      "description": "Production cluster"
    }
  }
}

API keys can also be referenced as environment variables:
    "api_key": "$PROD_KIBANA_API_KEY"
"""

import sys
import os
import requests
import logging
import json
import csv
import time
from collections import defaultdict
from argparse import ArgumentParser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==============================================================================
# CONFIGURATION
# ==============================================================================

def load_config(config_path):
    """
    Load cluster configuration from a JSON file.
    Resolves environment variable references in api_key values.

    Args:
        config_path (str): Path to the JSON config file

    Returns:
        dict: Parsed and resolved configuration
    """
    if not os.path.exists(config_path):
        logging.error(f"Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path, 'r') as f:
        config = json.load(f)

    clusters = config.get("clusters", {})
    if not clusters:
        logging.error("No clusters defined in config file.")
        sys.exit(1)

    # Resolve environment variable references for api_key
    for name, cluster in clusters.items():
        api_key = cluster.get("api_key", "")
        if api_key.startswith("$"):
            env_var = api_key[1:]
            resolved = os.environ.get(env_var)
            if not resolved:
                logging.warning(f"[{name}] Environment variable '{env_var}' not set. Skipping cluster.")
                cluster["api_key"] = None
            else:
                cluster["api_key"] = resolved

        # Default verify_ssl to True if not specified
        if "verify_ssl" not in cluster:
            cluster["verify_ssl"] = True

    return config


def validate_cluster_config(name, cluster):
    """Validate that a cluster config has required fields."""
    required = ["kibana_url", "api_key"]
    for field in required:
        if not cluster.get(field):
            logging.warning(f"[{name}] Missing or empty '{field}'. Skipping.")
            return False
    return True


# ==============================================================================
# KIBANA API HELPERS
# ==============================================================================

# Initialize Kibana object types to be processed
def get_object_types():
    object_types = [
        "config", "config-global", "url", "index-pattern", "action", "query",
        "tag", "graph-workspace", "alert", "search", "visualization",
        "event-annotation-group", "dashboard", "lens", "cases",
        "metrics-data-source", "links", "canvas-element", "canvas-workpad",
        "osquery-saved-query", "osquery-pack", "csp-rule-template", "map",
        "infrastructure-monitoring-log-view", "threshold-explorer-view",
        "uptime-dynamic-settings", "synthetics-privates-locations",
        "apm-indices", "infrastructure-ui-source", "inventory-view",
        "infra-custom-dashboards", "metrics-explorer-view", "apm-service-group",
        "apm-custom-dashboards"
    ]
    return object_types


def get_headers(api_key):
    """Set up headers for Kibana authentication."""
    return {
        'kbn-xsrf': 'true',
        'Content-Type': 'application/json',
        'Authorization': f'ApiKey {api_key}'
    }


def get_all_spaces(headers, kibana_url, verify_ssl=True):
    """
    Retrieve all Kibana spaces in a deployment.

    Args:
        headers (dict): Authentication headers
        kibana_url (str): Kibana base URL
        verify_ssl (bool): Whether to verify SSL certificates

    Returns:
        list: List of space dicts with 'id' and 'name' keys
    """
    spaces_endpoint = f"{kibana_url}/api/spaces/space"
    try:
        response = requests.get(spaces_endpoint, headers=headers, verify=verify_ssl, timeout=30)
        response.raise_for_status()
        spaces = response.json()
        logging.info(f"  Found {len(spaces)} spaces")
        return spaces
    except requests.exceptions.RequestException as e:
        logging.error(f"  Failed to retrieve spaces: {e}")
        return []


def get_all_dataviews(space_id, headers, kibana_url, verify_ssl=True):
    """
    Get all data views in the specified space.

    Args:
        space_id (str): Kibana space ID
        headers (dict): Authentication headers
        kibana_url (str): Kibana base URL
        verify_ssl (bool): Whether to verify SSL certificates

    Returns:
        list: List of data view dicts
    """
    dataview_url = f'{kibana_url}/s/{space_id}/api/data_views'
    try:
        response = requests.get(dataview_url, headers=headers, verify=verify_ssl, timeout=30)
        if response.status_code == 200:
            return response.json().get('data_view', [])
        else:
            logging.warning(f"    Failed to get data views in space '{space_id}': HTTP {response.status_code}")
            return []
    except requests.exceptions.RequestException as e:
        logging.warning(f"    Failed to get data views in space '{space_id}': {e}")
        return []


def find_duplicated_data_views(data_views):
    """
    Find data views with duplicate titles.

    Args:
        data_views (list): List of data view dicts

    Returns:
        dict: {title: [list of IDs]} for titles with more than one ID
    """
    title_to_ids = defaultdict(list)
    for dv in data_views:
        title_to_ids[dv["title"]].append(dv["id"])
    return {title: ids for title, ids in title_to_ids.items() if len(ids) > 1}


def get_object_references(data_view_ids, kibana_url, space_id, object_types, headers, verify_ssl=True):
    """
    Count saved-object references to each data view ID.

    Args:
        data_view_ids (list): Data view IDs to check references for
        kibana_url (str): Kibana base URL
        space_id (str): Space ID
        object_types (list): Kibana object types to scan
        headers (dict): Authentication headers
        verify_ssl (bool): Whether to verify SSL certificates

    Returns:
        dict: {data_view_id: reference_count}
    """
    objects_endpoint = f"{kibana_url}/s/{space_id}/api/saved_objects/_find"
    reference_counts = defaultdict(int)
    data_view_id_set = set(data_view_ids)

    for object_type in object_types:
        params = {
            'fields': 'references',
            'type': object_type,
            'per_page': 10000
        }
        try:
            response = requests.get(
                objects_endpoint, headers=headers, params=params,
                verify=verify_ssl, timeout=60
            )
            response.raise_for_status()
            data = response.json()

            for obj in data.get("saved_objects", []):
                for ref in obj.get("references", []):
                    if ref["type"] == "index-pattern" and ref["id"] in data_view_id_set:
                        reference_counts[ref["id"]] += 1

        except requests.exceptions.RequestException:
            # Silently continue — some object types may not exist in all spaces
            pass

    return reference_counts


# ==============================================================================
# CONNECTIVITY CHECK
# ==============================================================================

def check_connectivity(clusters):
    """
    Test connectivity to all configured clusters.

    Args:
        clusters (dict): Cluster configurations

    Returns:
        dict: {cluster_name: True/False}
    """
    results = {}
    for name, cluster in clusters.items():
        if not validate_cluster_config(name, cluster):
            results[name] = False
            continue

        headers = get_headers(cluster["api_key"])
        verify_ssl = cluster.get("verify_ssl", True)
        kibana_url = cluster["kibana_url"]

        try:
            response = requests.get(
                f"{kibana_url}/api/spaces/space",
                headers=headers, verify=verify_ssl, timeout=15
            )
            if response.status_code == 200:
                space_count = len(response.json())
                print(f"  ✅ {name:20s} — Connected ({space_count} spaces)")
                results[name] = True
            else:
                print(f"  ❌ {name:20s} — HTTP {response.status_code}")
                results[name] = False
        except requests.exceptions.RequestException as e:
            print(f"  ❌ {name:20s} — {e}")
            results[name] = False

    return results


# ==============================================================================
# CORE: SCAN A SINGLE CLUSTER
# ==============================================================================

def scan_cluster(name, cluster, object_types):
    """
    Scan a single cluster for duplicate data views across all spaces.

    Args:
        name (str): Cluster/deployment name
        cluster (dict): Cluster config dict
        object_types (list): Kibana object types to check references against

    Returns:
        list: List of result dicts, one per duplicate group found
    """
    kibana_url = cluster["kibana_url"]
    api_key = cluster["api_key"]
    verify_ssl = cluster.get("verify_ssl", True)
    headers = get_headers(api_key)
    results = []

    logging.info(f"[{name}] Scanning {kibana_url} ...")
    spaces = get_all_spaces(headers, kibana_url, verify_ssl)

    if not spaces:
        logging.warning(f"[{name}] No spaces found or unable to connect.")
        return results

    for space in spaces:
        space_id = space["id"]
        space_name = space.get("name", space_id)

        data_views = get_all_dataviews(space_id, headers, kibana_url, verify_ssl)
        if not data_views:
            continue

        duplicates = find_duplicated_data_views(data_views)
        if not duplicates:
            continue

        for title, ids in duplicates.items():
            reference_counts = get_object_references(
                ids, kibana_url, space_id, object_types, headers, verify_ssl
            )
            for dv_id in ids:
                results.append({
                    "deployment": name,
                    "kibana_url": kibana_url,
                    "space_id": space_id,
                    "space_name": space_name,
                    "data_view_title": title,
                    "data_view_id": dv_id,
                    "reference_count": reference_counts.get(dv_id, 0),
                    "duplicate_count": len(ids)
                })

    return results


# ==============================================================================
# OUTPUT / REPORTING
# ==============================================================================

def print_results(all_results):
    """Print results grouped by deployment and space."""
    if not all_results:
        print("\n" + "=" * 90)
        print("✅ ALL CLEAR: No duplicate data views found across any deployment.")
        print("=" * 90)
        return

    # Group results by deployment -> space -> title
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for r in all_results:
        grouped[r["deployment"]][r["space_name"]][r["data_view_title"]].append(r)

    total_duplicates = 0
    total_deployments_affected = len(grouped)

    print("\n" + "=" * 90)
    print("DUPLICATE DATA VIEWS REPORT")
    print("=" * 90)

    for deployment in sorted(grouped.keys()):
        spaces = grouped[deployment]
        print(f"\n{'─' * 90}")
        print(f"📦 DEPLOYMENT: {deployment.upper()}")
        print(f"{'─' * 90}")

        for space_name in sorted(spaces.keys()):
            titles = spaces[space_name]
            print(f"\n  🔹 Space: {space_name}")

            for title in sorted(titles.keys()):
                entries = titles[title]
                total_duplicates += 1
                print(f"\n    Data View Title: {title}")
                print(f"    Copies: {entries[0]['duplicate_count']}")
                for entry in entries:
                    ref_label = f"{entry['reference_count']} references"
                    print(f"      ID: {entry['data_view_id']:45s}  ({ref_label})")

    # Summary
    total_entries = len(all_results)
    print(f"\n{'=' * 90}")
    print("SUMMARY")
    print(f"{'=' * 90}")
    print(f"  Deployments with duplicates : {total_deployments_affected}")
    print(f"  Duplicate title groups       : {total_duplicates}")
    print(f"  Total duplicate data view IDs: {total_entries}")
    print(f"{'=' * 90}")


def export_csv(all_results, output_file):
    """Export results to CSV file."""
    if not all_results:
        print("No results to export.")
        return

    fieldnames = [
        "deployment", "space_id", "space_name", "data_view_title",
        "data_view_id", "reference_count", "duplicate_count"
    ]
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n📄 CSV report exported to: {output_file}")


def export_json(all_results, output_file):
    """Export results to JSON file."""
    if not all_results:
        print("No results to export.")
        return

    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n📄 JSON report exported to: {output_file}")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    parser = ArgumentParser(
        description='Find duplicate data views across multiple Kibana deployments.'
    )
    parser.add_argument(
        '--config', required=True,
        help='Path to the clusters JSON config file'
    )
    parser.add_argument(
        '--clusters', nargs='+', default=None,
        help='Specific cluster names to scan (default: scan all)'
    )
    parser.add_argument(
        '--output', choices=['table', 'csv', 'json'], default='table',
        help='Output format (default: table)'
    )
    parser.add_argument(
        '--output-file', default=None,
        help='Output file path for csv/json (auto-generated if not specified)'
    )
    parser.add_argument(
        '--connectivity-check', action='store_true',
        help='Only test connectivity to all clusters, then exit'
    )
    parser.add_argument(
        '--workers', type=int, default=1,
        help='Number of concurrent workers for scanning clusters (default: 1)'
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help='Enable verbose/debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    # Load config
    config = load_config(args.config)
    clusters = config["clusters"]

    # Filter to specific clusters if requested
    if args.clusters:
        filtered = {}
        for c in args.clusters:
            if c in clusters:
                filtered[c] = clusters[c]
            else:
                logging.warning(f"Cluster '{c}' not found in config. Available: {list(clusters.keys())}")
        clusters = filtered
        if not clusters:
            logging.error("No valid clusters to scan.")
            sys.exit(1)

    # Connectivity check mode
    if args.connectivity_check:
        print("\n🔌 CONNECTIVITY CHECK")
        print("=" * 60)
        results = check_connectivity(clusters)
        success = sum(1 for v in results.values() if v)
        total = len(results)
        print(f"\n  Result: {success}/{total} clusters reachable")
        sys.exit(0 if success == total else 1)

    # Validate all clusters before starting
    valid_clusters = {}
    for name, cluster in clusters.items():
        if validate_cluster_config(name, cluster):
            valid_clusters[name] = cluster

    if not valid_clusters:
        logging.error("No valid clusters to scan after validation.")
        sys.exit(1)

    object_types = get_object_types()
    all_results = []

    print(f"\n🔍 Scanning {len(valid_clusters)} deployment(s) for duplicate data views...\n")
    start_time = time.time()

    # Sequential or concurrent execution
    if args.workers > 1 and len(valid_clusters) > 1:
        logging.info(f"Using {args.workers} concurrent workers")
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(scan_cluster, name, cluster, object_types): name
                for name, cluster in valid_clusters.items()
            }
            for future in as_completed(futures):
                cluster_name = futures[future]
                try:
                    results = future.result()
                    all_results.extend(results)
                except Exception as e:
                    logging.error(f"[{cluster_name}] Scan failed: {e}")
    else:
        for name, cluster in valid_clusters.items():
            try:
                results = scan_cluster(name, cluster, object_types)
                all_results.extend(results)
            except Exception as e:
                logging.error(f"[{name}] Scan failed: {e}")

    elapsed = time.time() - start_time
    logging.info(f"Scan completed in {elapsed:.1f} seconds")

    # Output results
    if args.output == 'table' or args.output == 'csv':
        print_results(all_results)

    if args.output == 'csv':
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = args.output_file or f"duplicate_dataviews_{timestamp}.csv"
        export_csv(all_results, output_file)

    if args.output == 'json':
        print_results(all_results)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = args.output_file or f"duplicate_dataviews_{timestamp}.json"
        export_json(all_results, output_file)


if __name__ == "__main__":
    main()
