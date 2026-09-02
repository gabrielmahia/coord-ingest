# coord-ingest

Global situational-awareness dashboards ingest hundreds of live feeds and turn them into an actionable picture — but they score the world's Tier-1 countries, and East Africa isn't on that list. The signals that matter here (drought onset, basin flooding, cholera clusters, rift-zone quakes) are in the same public feeds; nothing was turning them into events a regional coordination system could act on.

`coord-ingest` is that missing rail: an MIT-licensed adapter that converts open, key-free world-signal feeds into [`africa-coord-bus`](https://github.com/gabrielmahia/africa-coord-bus) coordination events, **filtered to East Africa**, so the bus can cascade them to the right domains (insurance, crop advisory, water testing, cross-border alerts).

It ingests and normalizes; the bus routes. Clean separation, composable rails.

## What it does

- **Adapters** for public sources — Open-Meteo rainfall (drought/flood signals), USGS earthquakes, GDACS disaster alerts, HDX humanitarian datasets (OCHA, key-free CKAN), Kobo/ODK field reports (bottom-up ground truth), Google Flood Hub riverine forecasts — each mapping raw records to typed `CoordinationEvent`s. Add a feed by adding an adapter.
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
# from coord_ingest import OpenMeteoAdapter, USGSQuakeAdapter, GDACSAdapter
# from africa_coord_bus import EventBus
# IngestPipeline([USGSQuakeAdapter(), GDACSAdapter()]).run(bus=EventBus(...))
```

## Why not just fork a dashboard?

The global open-source dashboards that inspired this are AGPL applications. This is deliberately the opposite: a small **MIT library** that produces standard bus events any tool can consume — including a dashboard, if someone wants to build one. Rails, not another train.

## IP & Collaboration

MIT. New feed adapters welcome via GitHub Issues (public, key-free sources, East-Africa relevant). Full policy: [docs/architecture/IP_POLICY.md](docs/architecture/IP_POLICY.md). Security: [SECURITY.md](SECURITY.md).

## Anticipatory action: why forecasts belong on a bus

The Flood Hub adapter exists because a flood forecast is only worth what it
triggers. Google's models cover 40 African countries with riverine forecasts up
to 7 days ahead (CC BY 4.0, free), and forecast points now exist in Northern
Kenya where there were none. IRC and GiveDirectly used those forecasts to move
cash to households 5-7 days *before* flood peaks in Nigeria — the money arrived
while it could still be spent on preparation rather than recovery.

That is a cascade, not a dashboard: a verified gauge crossing a danger threshold
is a water-domain signal whose value is downstream — finance (anticipatory
transfer), health (facility pre-positioning), transport (route closure).
`coord-ingest` emits the signal; `africa-coord-bus` routes it.

```python
from coord_ingest import FloodHubAdapter, IngestPipeline
events = IngestPipeline([FloodHubAdapter()]).collect()   # needs FLOODHUB_API_KEY
```

**Access:** the Flood Forecasting API is waitlisted — approval plus a Google
Cloud Project ID, currently non-commercial use only. Without a key the adapter
returns nothing and the pipeline still runs on its other feeds. Unverified
gauges are excluded by default: an unverified gauge triggering a cash transfer
is a worse failure than a missed alert.
