"""IngestPipeline — run adapters, filter to East Africa, hand events to the bus.

The pipeline is deliberately thin: adapters produce events, the pipeline drops
anything outside the region, and routing/cascading stays in africa-coord-bus.
This keeps the ingest rail composable and the bus authoritative.
"""

from __future__ import annotations

from africa_coord_bus.event import CoordinationEvent

# Rough East Africa bounding box (lat_min, lat_max, lon_min, lon_max).
# Covers Kenya, Tanzania, Uganda, Rwanda, Burundi, Ethiopia, Somalia, S. Sudan.
EAST_AFRICA_BBOX = (-12.0, 15.0, 28.0, 52.0)

EAST_AFRICA_COUNTRIES = {
    "kenya", "tanzania", "uganda", "rwanda", "burundi", "ethiopia",
    "somalia", "south sudan", "sudan", "djibouti", "eritrea",
}


def in_east_africa(event: CoordinationEvent) -> bool:
    """True if the event's location falls in the region (by bbox or country)."""
    data = event.data or {}
    country = str(data.get("country") or "").strip().lower()
    if country and country in EAST_AFRICA_COUNTRIES:
        return True
    lat, lon = data.get("lat"), data.get("lon")
    if lat is None or lon is None:
        # No geo and no known country -> cannot confirm region; drop conservatively.
        return False
    lat_min, lat_max, lon_min, lon_max = EAST_AFRICA_BBOX
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


class IngestPipeline:
    """Collects events from adapters, filters to East Africa, optionally routes."""

    def __init__(self, adapters, region_filter=in_east_africa):
        self.adapters = list(adapters)
        self.region_filter = region_filter

    def collect(self) -> list[CoordinationEvent]:
        """All region-relevant events across every adapter."""
        out: list[CoordinationEvent] = []
        for adapter in self.adapters:
            try:
                events = adapter.to_events()
            except Exception:  # noqa: BLE001 — one bad feed must not sink the run
                continue
            out.extend(e for e in events if self.region_filter(e))
        return out

    def run(self, bus=None) -> dict:
        """Collect region events; if a bus is given, publish each and return a
        summary. Without a bus, returns the events for inspection."""
        events = self.collect()
        summary = {"collected": len(events), "by_domain": {}, "published": 0}
        for e in events:
            d = e.domain.value
            summary["by_domain"][d] = summary["by_domain"].get(d, 0) + 1
        if bus is not None:
            for e in events:
                try:
                    bus.publish(e)
                    summary["published"] += 1
                except Exception:  # noqa: BLE001
                    pass
        summary["events"] = events
        return summary
