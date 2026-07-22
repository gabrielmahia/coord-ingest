"""coord-ingest — turn open world-signal feeds into coordination events.

WHY THIS EXISTS
---------------
The coordination stack can *route* events (africa-coord-bus) and *serve* domain
data (35 MCP servers), but nothing turned raw, real-world signals INTO
coordination events. Dashboards like worldmonitor prove the ingest+correlate
pattern at global scale — but they are AGPL applications scored on Tier-1
countries, and East Africa is not Tier-1. This is the missing *rail*: an
MIT-licensed adapter that maps public feeds (USGS quakes, ReliefWeb disasters,
GDACS alerts, WHO outbreaks) into typed CoordinationEvents the bus already knows
how to cascade — scoped to the region the global dashboards skip.

DESIGN
------
- **Source-agnostic.** Each adapter implements `FeedAdapter.fetch() -> list[dict]`
  and `.to_events() -> list[CoordinationEvent]`. Add a feed by adding an adapter.
- **East-Africa filtered.** A signal only becomes an event if it lands in the
  region bounding box or names a regional country. Global noise is dropped at
  ingest, not routing.
- **No keys, offline-capable.** Ships with a `SampleAdapter` (bundled fixtures)
  so the whole pipeline runs and tests without network — the same local-first
  ethos, applied to our region.
- **Feeds the bus, doesn't replace it.** Output is CoordinationEvents; routing,
  cascading, and cross-border logic stay in africa-coord-bus where they belong.
"""

from __future__ import annotations

from .adapters import (
    FeedAdapter,
    SampleAdapter,
    USGSQuakeAdapter,
    ReliefWebAdapter,
    GDACSAdapter,
    OpenMeteoAdapter,
)
from .pipeline import IngestPipeline, EAST_AFRICA_BBOX, in_east_africa

__version__ = "0.1.0"

__all__ = [
    "FeedAdapter",
    "SampleAdapter",
    "USGSQuakeAdapter",
    "ReliefWebAdapter",
    "GDACSAdapter",
    "OpenMeteoAdapter",
    "IngestPipeline",
    "EAST_AFRICA_BBOX",
    "in_east_africa",
]
