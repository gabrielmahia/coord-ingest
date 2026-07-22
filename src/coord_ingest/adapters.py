"""Feed adapters: public world-signal source -> CoordinationEvent.

Each adapter is small and single-purpose. Real network adapters fetch public,
key-free feeds; SampleAdapter uses bundled fixtures so the pipeline runs offline.
All adapters emit africa-coord-bus CoordinationEvents — the bus owns routing.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from importlib import resources

from africa_coord_bus.event import CoordinationEvent, EventDomain, EventSeverity


def _severity_from_magnitude(mag: float) -> EventSeverity:
    if mag >= 7.0:
        return EventSeverity.CRITICAL
    if mag >= 6.0:
        return EventSeverity.ALERT
    if mag >= 5.0:
        return EventSeverity.WARNING
    return EventSeverity.INFO


class FeedAdapter(ABC):
    """Base contract: fetch raw records, convert to CoordinationEvents."""

    #: bus domain this feed primarily maps to
    domain: EventDomain = EventDomain.CIVIC
    #: event_type emitted (must match routing-table triggers to cascade)
    event_type: str = "external_signal"
    #: stable source label
    source: str = "coord-ingest"

    @abstractmethod
    def fetch(self) -> list[dict]:
        """Return raw feed records as dicts. Network or fixtures."""

    def to_events(self) -> list[CoordinationEvent]:
        events = []
        for rec in self.fetch():
            ev = self._record_to_event(rec)
            if ev is not None:
                events.append(ev)
        return events

    @abstractmethod
    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        """Map one raw record to a CoordinationEvent (or None to drop)."""


class SampleAdapter(FeedAdapter):
    """Offline adapter over bundled fixtures — runs the whole pipeline with no
    network and no keys. Fixtures cover quake, flood, and outbreak signals in
    East Africa so cascades can be demonstrated end to end."""

    domain = EventDomain.WATER
    event_type = "external_signal"
    source = "coord-ingest.sample"

    def fetch(self) -> list[dict]:
        text = resources.files("coord_ingest").joinpath("data/sample_feed.json").read_text("utf-8")
        return json.loads(text)

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        domain = EventDomain(rec["domain"])
        return CoordinationEvent(
            domain=domain,
            event_type=rec["event_type"],
            source=self.source,
            severity=EventSeverity(rec.get("severity", "warning")),
            data={
                "title": rec.get("title", ""),
                "lat": rec.get("lat"),
                "lon": rec.get("lon"),
                "country": rec.get("country"),
                "border_adjacent": rec.get("border_adjacent", False),
                "origin_feed": "sample",
            },
        )


class USGSQuakeAdapter(FeedAdapter):
    """USGS earthquakes (public GeoJSON, no key). Quakes near dams, lakes, or
    settlements are routed as water/transport signals for downstream assessment."""

    domain = EventDomain.TRANSPORT
    event_type = "infrastructure_shock"
    source = "coord-ingest.usgs"
    FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"

    def __init__(self, records: list[dict] | None = None):
        # allow injected records for testing without network
        self._injected = records

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import urllib.request

        with urllib.request.urlopen(self.FEED_URL, timeout=20) as r:
            payload = json.load(r)
        return payload.get("features", [])

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        props = rec.get("properties", {})
        geom = rec.get("geometry", {})
        coords = geom.get("coordinates", [None, None])
        lon, lat = coords[0], coords[1]
        mag = props.get("mag") or 0.0
        return CoordinationEvent(
            domain=self.domain,
            event_type=self.event_type,
            source=self.source,
            severity=_severity_from_magnitude(mag),
            data={
                "title": props.get("place", ""),
                "magnitude": mag,
                "lat": lat,
                "lon": lon,
                "origin_feed": "usgs",
            },
        )


class ReliefWebAdapter(FeedAdapter):
    """ReliefWeb disaster reports (public API). Maps humanitarian disaster
    signals to the water/health domains for coordination follow-up."""

    domain = EventDomain.HEALTH
    event_type = "disaster_report"
    source = "coord-ingest.reliefweb"

    def __init__(self, records: list[dict] | None = None):
        self._injected = records

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import urllib.request

        url = ("https://api.reliefweb.int/v1/disasters?appname=coord-ingest"
               "&profile=list&preset=latest&limit=20")
        with urllib.request.urlopen(url, timeout=20) as r:
            return json.load(r).get("data", [])

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        fields = rec.get("fields", {})
        name = fields.get("name", "")
        return CoordinationEvent(
            domain=self.domain,
            event_type=self.event_type,
            source=self.source,
            severity=EventSeverity.WARNING,
            data={"title": name, "origin_feed": "reliefweb"},
        )


class GDACSAdapter(FeedAdapter):
    """GDACS global disaster alerts. Alert colour maps to severity; the pipeline
    filters to East Africa downstream."""

    domain = EventDomain.WATER
    event_type = "disaster_alert"
    source = "coord-ingest.gdacs"
    _COLOUR = {"Green": EventSeverity.INFO, "Orange": EventSeverity.ALERT, "Red": EventSeverity.CRITICAL}

    def __init__(self, records: list[dict] | None = None):
        self._injected = records

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import urllib.request

        with urllib.request.urlopen("https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH", timeout=20) as r:
            return json.load(r).get("features", [])

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        props = rec.get("properties", rec)
        colour = props.get("alertlevel", "Green")
        return CoordinationEvent(
            domain=self.domain,
            event_type=self.event_type,
            source=self.source,
            severity=self._COLOUR.get(colour, EventSeverity.INFO),
            data={
                "title": props.get("eventname") or props.get("description", ""),
                "lat": props.get("latitude"),
                "lon": props.get("longitude"),
                "country": props.get("country"),
                "origin_feed": "gdacs",
            },
        )
