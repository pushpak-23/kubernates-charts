# Solr Collection Placement Watcher

This repository adds a small standalone controller that watches Solr for newly created collections and then places replicas on the matching node pools.

How it behaves:

- It snapshots the current collection list on startup and treats that as the baseline.
- It only reacts to collections created after the watcher starts.
- For brand-new empty collections, it can delete a misplaced managed replica and recreate it on the right node pool.
- For collections that already contain data, it stays add-only and never deletes replicas.
- It retries until a collection is visible in cluster status, so it works right after collection creation.

The default rules in [k8s/solr-collection-watcher.yaml](k8s/solr-collection-watcher.yaml) are:

- Add one `PULL` replica per shard on nodes whose name matches `solr-cloud-pull-solrcloud`.
- Add one `NRT` replica per shard on nodes whose name matches `solr-cloud-nrt-solrcloud`.

Build the image:

```bash
docker build -t ghcr.io/your-org/solr-collection-watcher:latest .
```

Deploy it:

```bash
kubectl apply -f k8s/solr-collection-watcher.yaml
```

If your Solr service name or node naming scheme is different, update the environment variables in the Deployment before applying it.

Notes

- The `Dockerfile` at the repository root builds the watcher image.
- The `k8s/solr-collection-watcher.yaml` Deployment contains example environment variables; edit them for your cluster (Service DNS, node name patterns, image name).
