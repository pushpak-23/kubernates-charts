# Solr Cloud Kubernetes Manifests

This folder contains Kubernetes manifests and Helm values for deploying an Apache Solr Cloud setup (operator, NRT and PULL SolrClouds, load-balancers, storageclass, and a shared Zookeeper cluster).

All Kubernetes manifests are in the `k8s/` subdirectory to keep the repository organized.

Contents (in `k8s/`):

- `solr-operator.yaml` — Helm values for the `solr-operator` chart.
- `solr-nrt-production.yaml` — SolrCloud manifest for indexing (NRT) nodes.
- `solr-pull-production.yaml` — SolrCloud manifest for search (PULL) nodes.
- `solr-nrt-lb.yaml` — LoadBalancer Service for the NRT pool.
- `solr-query-lb.yaml` — LoadBalancer Service for the query/pull pool.
- `solr-ssd-xfs-sc.yaml` — StorageClass used by Solr PVCs.
- `zookeeper-shared.yaml` — Shared Zookeeper cluster used by both SolrClouds.

Quick install (Helm operator):

```bash
helm repo add apache-solr https://solr.apache.org/charts
helm repo update
helm install solr apache-solr/solr-operator \
  --namespace solr-system \
  --create-namespace \
  --timeout 10m0s \
  --values k8s/solr-operator.yaml \
  --version 0.9.1 \
  --wait
```

Configure Affinity Placement Plugin (one-time)

Run this curl command once (on any Solr node that can reach the Solr operator API, preferably an indexing node):

```bash
curl -X POST -H 'Content-type: application/json' \
  http://solr-cloud-nrt.solr-system.svc.cluster.local:8983/api/cluster/plugin \
  -d '{
  "add": {
    "name": ".placement-plugin",
    "class": "org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory",
    "config": {
      "collectionNodeType": {
        "mycollection": "indexing,search"
      }
    }
  }
}'
```

Notes

- Tune JVM and resource settings in the `k8s/` manifests to match your cluster capacity.
- If your node names or labels differ from the examples, update `nodeSelector` / `affinity` sections before applying the manifests.
