# coord-ingest

Global situational-awareness dashboards ingest hundreds of live feeds and turn them into an actionable picture — but they score the world's Tier-1 countries, and East Africa isn't on that list. The signals that matter here (drought onset, basin flooding, cholera clusters, rift-zone quakes) are in the same public feeds; nothing was turning them into events a regional coordination system could act on.

`coord-ingest` is that missing rail: an MIT-licensed adapter that converts open, key-free world-signal feeds into [`africa-coord-bus`](https://github.com/gabrielmahia/africa-coord-bus) coordination events, **filtered to East Africa**, so the bus can cascade them to the right domains (insurance, crop advisory, water testing, cross-border alerts).

It ingests and normalizes; the bus routes. Clean separation, composable rails.

## What it does

- **Adapters** for public sources — USGS earthquakes, ReliefWeb disasters, GDACS alerts — each mapping raw records to typed `CoordinationEvent`s. Add a feed by adding an adapter.
- **East-Africa filter** — a signal becomes an event only if it lands in the regional bounding box or names a regional country. Global noise is dropped at ingest.
- **Offline-capable** — ships with a `SampleAdapter` over bundled fixtures, so the full pipeline (and its tests) run with no network and no API keys.
- **Feeds the bus, doesn't replace it** — routing, cascading, and cross-border logic stay in `africa-coord-bus`.

## Use it

```bash
pip install coord-ingest
```

```python
from coord_ingest import IngestPipeline, SampleAdapter

# Offline demo — no keys, no network
summary = IngestPipeline([SampleAdapter()]).run()
print(summary["collected"], "East Africa events")   # global noise already dropped
print(summary["by_domain"])                          # {'water': 3, 'health': 1, 'transport': 1}

# With live feeds + the bus:
# from coord_ingest import USGSQuakeAdapter, GDACSAdapter
# from africa_coord_bus import EventBus
# IngestPipeline([USGSQuakeAdapter(), GDACSAdapter()]).run(bus=EventBus(...))
```

## Why not just fork a dashboard?

The global open-source dashboards that inspired this are AGPL applications. This is deliberately the opposite: a small **MIT library** that produces standard bus events any tool can consume — including a dashboard, if someone wants to build one. Rails, not another train.

## IP & Collaboration

MIT. New feed adapters welcome via GitHub Issues (public, key-free sources, East-Africa relevant). Full policy: [docs/architecture/IP_POLICY.md](docs/architecture/IP_POLICY.md). Security: [SECURITY.md](SECURITY.md).
