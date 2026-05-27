#!/usr/bin/env python3

import json
import logging
import os
import re
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("solr-collection-watcher")


@dataclass(frozen=True)
class ReplicaRule:
    name: str
    node_regex: re.Pattern[str]
    replica_type: str
    replicas_per_shard: int


class SolrClient:
    def __init__(self, base_url: str, timeout_seconds: int = 20) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout_seconds = timeout_seconds

    def _request_json(self, path: str, params: Dict[str, str]) -> Dict[str, object]:
        url = urljoin(self.base_url, path.lstrip("/"))
        if params:
            url = f"{url}?{urlencode(params)}"
        request = Request(url, headers={"Accept": "application/json"})

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise RuntimeError(f"Solr request failed: {exc.code} {exc.reason} {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"Solr request failed: {exc.reason}") from exc

    def list_collections(self) -> Set[str]:
        payload = self._request_json("admin/collections", {"action": "LIST", "wt": "json"})
        collections = payload.get("collections", [])
        return {str(collection) for collection in collections}

    def cluster_status(self, collection: str) -> Dict[str, object]:
        return self._request_json(
            "admin/collections",
            {"action": "CLUSTERSTATUS", "collection": collection, "wt": "json"},
        )

    def add_replica(self, collection: str, shard: str, replica_type: str, node: str) -> Dict[str, object]:
        return self._request_json(
            "admin/collections",
            {
                "action": "ADDREPLICA",
                "collection": collection,
                "shard": shard,
                "type": replica_type,
                "node": node,
                "wt": "json",
            },
        )

    def delete_replica(self, collection: str, shard: str, replica: str) -> Dict[str, object]:
        return self._request_json(
            "admin/collections",
            {
                "action": "DELETEREPLICA",
                "collection": collection,
                "shard": shard,
                "replica": replica,
                "wt": "json",
            },
        )

    def move_replica(self, collection: str, replica: str, target_node: str) -> Dict[str, object]:
        return self._request_json(
            "admin/collections",
            {
                "action": "MOVEREPLICA",
                "collection": collection,
                "replica": replica,
                "targetNode": target_node,
                "wt": "json",
            },
        )

    def replica_exists_on_node(self, collection: str, shard: str, replica_type_name: str, node: str) -> bool:
        cluster_status = self.cluster_status(collection)
        shards = extract_shards(cluster_status, collection)
        shard_info = shards.get(shard, {})
        replicas = shard_info.get("replicas", {}) if isinstance(shard_info, dict) else {}
        if not isinstance(replicas, dict):
            return False

        for replica in replicas.values():
            if not isinstance(replica, dict):
                continue
            if replica_type(replica) != replica_type_name.upper():
                continue
            if replica_node_name(replica) != node:
                continue
            if str(replica.get("state", "")).upper() == "ACTIVE":
                return True
        return False

    def collection_document_count(self, collection: str) -> int:
        payload = self._request_json(f"{collection}/select", {"q": "*:*", "rows": "0", "wt": "json"})
        response = payload.get("response", {})
        if not isinstance(response, dict):
            raise RuntimeError(f"Unexpected Solr query response for collection {collection}")
        return int(response.get("numFound", 0))


def load_state(state_file: Path) -> Set[str]:
    if not state_file.exists():
        return set()

    with state_file.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    seen = data.get("seen_collections", [])
    return {str(collection) for collection in seen}


def save_state(state_file: Path, seen_collections: Set[str]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"seen_collections": sorted(seen_collections)}
    with state_file.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def parse_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def build_rule(
    name: str,
    regex_env: str,
    type_env: str,
    count_env: str,
    default_regex: str,
    default_type: str,
    default_count: int,
) -> Optional[ReplicaRule]:
    replicas_per_shard = parse_int_env(count_env, default_count)
    if replicas_per_shard <= 0:
        return None

    pattern = re.compile(os.getenv(regex_env, default_regex))
    replica_type = os.getenv(type_env, default_type).upper()
    return ReplicaRule(name=name, node_regex=pattern, replica_type=replica_type, replicas_per_shard=replicas_per_shard)


def extract_shards(cluster_status: Dict[str, object], collection: str) -> Dict[str, Dict[str, object]]:
    cluster = cluster_status.get("cluster", {})
    collections = cluster.get("collections", {}) if isinstance(cluster, dict) else {}
    collection_info = collections.get(collection, {}) if isinstance(collections, dict) else {}
    shards = collection_info.get("shards", {}) if isinstance(collection_info, dict) else {}
    if not isinstance(shards, dict):
        return {}
    return shards  # type: ignore[return-value]


def extract_live_nodes(cluster_status: Dict[str, object]) -> List[str]:
    cluster = cluster_status.get("cluster", {})
    live_nodes = cluster.get("live_nodes", []) if isinstance(cluster, dict) else []
    if not isinstance(live_nodes, list):
        return []
    return [str(node) for node in live_nodes]


def replica_node_name(replica: Dict[str, object]) -> str:
    value = replica.get("node_name") or replica.get("node") or ""
    return str(value)


def replica_type(replica: Dict[str, object]) -> str:
    return str(replica.get("type", "")).upper()


def choose_target_nodes(live_nodes: Sequence[str], rule: ReplicaRule) -> List[str]:
    return [node for node in live_nodes if rule.node_regex.search(node)]


def wait_for_replica_on_node(solr: SolrClient, collection: str, shard: str, replica_type_name: str, node: str, timeout_seconds: int = 180) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if solr.replica_exists_on_node(collection, shard, replica_type_name, node):
                return True
        except Exception as exc:  # noqa: BLE001 - retry until Solr shows the new placement
            logger.debug(
                "Waiting for %s replica on %s for collection %s shard %s: %s",
                replica_type_name,
                node,
                collection,
                shard,
                exc,
            )
        time.sleep(3)
    return False


def ensure_rule_for_collection(solr: SolrClient, collection: str, rule: ReplicaRule, allow_relocation: bool) -> None:
    cluster_status = solr.cluster_status(collection)
    live_nodes = extract_live_nodes(cluster_status)
    target_nodes = choose_target_nodes(live_nodes, rule)

    if not target_nodes:
        logger.warning("No live nodes matched rule %s for collection %s", rule.name, collection)
        return

    shards = extract_shards(cluster_status, collection)
    if not shards:
        logger.warning("Collection %s has no shard information yet", collection)
        return

    for shard_name, shard_info in shards.items():
        replicas = shard_info.get("replicas", {}) if isinstance(shard_info, dict) else {}
        if not isinstance(replicas, dict):
            continue

        misplaced_replicas: List[Dict[str, object]] = []
        matched_nodes: Set[str] = set()
        matching_replica_count = 0
        for replica_name, replica in replicas.items():
            if not isinstance(replica, dict):
                continue
            if replica_type(replica) != rule.replica_type:
                continue
            matching_replica_count += 1
            node_name = replica_node_name(replica)
            if node_name in target_nodes:
                matched_nodes.add(node_name)
            else:
                misplaced_replicas.append({"replica_name": replica_name, **replica})

        if allow_relocation and misplaced_replicas:
                available_nodes = [node for node in target_nodes if node not in matched_nodes]
                if available_nodes:
                    primary_target = available_nodes[0]
                    primary_replica = misplaced_replicas.pop(0)
                    primary_replica_name = str(primary_replica.get("replica_name") or "")
                    if primary_replica_name:
                        logger.info(
                            "Moving misplaced %s replica %s for collection %s shard %s to node %s",
                            rule.replica_type,
                            primary_replica_name,
                            collection,
                            shard_name,
                            primary_target,
                        )
                        solr.move_replica(collection, primary_replica_name, primary_target)
                        if not wait_for_replica_on_node(solr, collection, shard_name, rule.replica_type, primary_target):
                            logger.warning(
                                "Moved %s replica %s for collection %s shard %s is not visible on %s yet; skipping cleanup for this shard",
                                rule.replica_type,
                                primary_replica_name,
                                collection,
                                shard_name,
                                primary_target,
                            )
                            continue

                for replica in misplaced_replicas:
                    replica_name = str(replica.get("replica_name") or "")
                    if not replica_name:
                        logger.warning(
                            "Skipping misplaced %s replica on collection %s shard %s because its replica name is missing",
                            rule.replica_type,
                            collection,
                            shard_name,
                        )
                        continue

                    logger.info(
                        "Deleting extra misplaced %s replica %s for collection %s shard %s",
                        rule.replica_type,
                        replica_name,
                        collection,
                        shard_name,
                    )
                    solr.delete_replica(collection, shard_name, replica_name)

                cluster_status = solr.cluster_status(collection)
                live_nodes = extract_live_nodes(cluster_status)
                target_nodes = choose_target_nodes(live_nodes, rule)
                shards = extract_shards(cluster_status, collection)
                shard_info = shards.get(shard_name, {})
                replicas = shard_info.get("replicas", {}) if isinstance(shard_info, dict) else {}
                if not isinstance(replicas, dict):
                    continue

                matched_nodes = set()
                matching_replica_count = 0
                for replica in replicas.values():
                    if not isinstance(replica, dict):
                        continue
                    if replica_type(replica) != rule.replica_type:
                        continue
                    matching_replica_count += 1
                    node_name = replica_node_name(replica)
                    if node_name in target_nodes:
                        matched_nodes.add(node_name)

        missing = rule.replicas_per_shard - matching_replica_count
        if missing <= 0:
            logger.info(
                "Collection %s shard %s already has %d %s replica(s)",
                collection,
                shard_name,
                rule.replicas_per_shard,
                rule.replica_type,
            )
            continue

        available_nodes = [node for node in target_nodes if node not in matched_nodes]
        if not available_nodes:
            logger.warning(
                "Collection %s shard %s needs %d more %s replica(s), but no unused %s nodes are available",
                collection,
                shard_name,
                missing,
                rule.replica_type,
                rule.name,
            )
            continue

        for node in available_nodes[:missing]:
            logger.info(
                "Adding %s replica for collection %s shard %s on node %s",
                rule.replica_type,
                collection,
                shard_name,
                node,
            )
            solr.add_replica(collection, shard_name, rule.replica_type, node)


def wait_for_collection(solr: SolrClient, collection: str, timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            solr.cluster_status(collection)
            return True
        except Exception as exc:  # noqa: BLE001 - retry until the collection is visible
            logger.debug("Waiting for collection %s to become ready: %s", collection, exc)
            time.sleep(5)
    return False


def is_empty_collection(solr: SolrClient, collection: str) -> bool:
    try:
        return solr.collection_document_count(collection) == 0
    except Exception as exc:  # noqa: BLE001 - avoid deleting anything if the emptiness check fails
        logger.warning("Unable to verify whether collection %s is empty: %s", collection, exc)
        return False


def process_collection(solr: SolrClient, collection: str, rules: Sequence[ReplicaRule], ready_timeout_seconds: int) -> None:
    logger.info("Detected new collection %s", collection)
    if not wait_for_collection(solr, collection, ready_timeout_seconds):
        logger.warning("Collection %s never became queryable within %s seconds", collection, ready_timeout_seconds)
        return

    empty_collection = is_empty_collection(solr, collection)
    logger.info("Collection %s empty state: %s", collection, empty_collection)

    for rule in rules:
        ensure_rule_for_collection(solr, collection, rule, allow_relocation=empty_collection)


def main() -> int:
    solr_base_url = os.getenv("SOLR_BASE_URL", "http://solr-nrt-lb.solr-system.svc.cluster.local:8983/solr")
    poll_interval_seconds = parse_int_env("POLL_INTERVAL_SECONDS", 30)
    ready_timeout_seconds = parse_int_env("COLLECTION_READY_TIMEOUT_SECONDS", 180)
    state_file = Path(os.getenv("STATE_FILE", "/state/seen-collections.json"))

    rules: List[ReplicaRule] = []
    pull_rule = build_rule(
        "pull",
        "PULL_NODE_REGEX",
        "PULL_REPLICA_TYPE",
        "PULL_REPLICAS_PER_SHARD",
        "solr-cloud-pull-solrcloud",
        "PULL",
        1,
    )
    query_rule = build_rule(
        "query",
        "QUERY_NODE_REGEX",
        "QUERY_REPLICA_TYPE",
        "QUERY_REPLICAS_PER_SHARD",
        "solr-cloud-nrt-solrcloud",
        "NRT",
        2,
    )
    for rule in (pull_rule, query_rule):
        if rule is not None:
            rules.append(rule)

    if not rules:
        logger.error("No replica rules are enabled")
        return 1

    solr = SolrClient(solr_base_url)

    stop_requested = False

    def handle_stop(signum: int, frame: object) -> None:  # noqa: ARG001
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    seen_collections = load_state(state_file)
    logger.info("Loaded %d previously seen collection(s)", len(seen_collections))

    if not seen_collections and not state_file.exists():
        try:
            current_collections = solr.list_collections()
        except Exception as exc:  # noqa: BLE001 - startup should fail loudly if Solr is unreachable
            logger.error("Unable to read the current collection list: %s", exc)
            return 1

        seen_collections = set(current_collections)
        save_state(state_file, seen_collections)
        logger.info("Baseline saved with %d existing collection(s)", len(seen_collections))

    while not stop_requested:
        try:
            current_collections = solr.list_collections()
        except Exception as exc:  # noqa: BLE001 - keep the controller alive across transient outages
            logger.warning("Unable to list collections: %s", exc)
            time.sleep(poll_interval_seconds)
            continue

        new_collections = sorted(current_collections - seen_collections)
        if new_collections:
            logger.info("Found %d new collection(s): %s", len(new_collections), ", ".join(new_collections))
        for collection in new_collections:
            process_collection(solr, collection, rules, ready_timeout_seconds)

        if new_collections:
            seen_collections |= set(new_collections)
            save_state(state_file, seen_collections)

        time.sleep(poll_interval_seconds)

    logger.info("Shutdown requested")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())