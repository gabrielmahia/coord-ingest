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

        # ReliefWeb v1 GET was retired (HTTP 410). v2 requires a POST query.
        # We degrade gracefully: on any failure, return [] so the pipeline runs
        # on its other adapters rather than crashing. Verified 2026-07-21.
        url = "https://api.reliefweb.int/v2/disasters?appname=coord-ingest&limit=20"
        body = json.dumps({"preset": "latest", "profile": "list"}).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r).get("data", [])
        except Exception:
            return []

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
        geom = rec.get("geometry", {})
        coords = geom.get("coordinates", [None, None]) if geom else [None, None]
        lon, lat = (coords[0], coords[1]) if len(coords) >= 2 else (None, None)
        colour = props.get("alertlevel", "Green")
        # GDACS country is a comma-separated string; keep it raw, the region
        # filter matches any regional country substring via the bbox on lat/lon.
        return CoordinationEvent(
            domain=self.domain,
            event_type=self.event_type,
            source=self.source,
            severity=self._COLOUR.get(colour, EventSeverity.INFO),
            data={
                "title": props.get("eventname") or props.get("name") or props.get("description", ""),
                "event_class": props.get("eventtype"),
                "lat": lat,
                "lon": lon,
                "country": props.get("country"),
                "origin_feed": "gdacs",
            },
        )

class OpenMeteoAdapter(FeedAdapter):
    """Open-Meteo forecast (public, key-free, CORS-friendly). Converts rainfall
    forecasts into drought/flood coordination events for monitored locations.

    This is the *upstream* adapter: drought and flood cascades all begin with a
    rainfall signal, and Open-Meteo provides it globally with no key. Monitored
    points default to East African population/agricultural centres; pass your own
    ``locations`` as (name, lat, lon, country) tuples to watch different places.

    Thresholds are deliberately simple and documented, not black-box:
      - 7-day precip < DROUGHT_MM  -> drought_alert (severity by how dry)
      - any day       > FLOOD_MM   -> flood_alert   (severity by peak)
    Tune for local seasonality before operational use.
    """

    domain = EventDomain.WATER
    source = "coord-ingest.openmeteo"

    DROUGHT_MM = 5.0    # 7-day total below this in a rainy period = drought signal
    FLOOD_MM = 50.0     # single-day total above this = flood signal

    DEFAULT_LOCATIONS = [
        ("Nairobi", -1.29, 36.82, "Kenya"),
        ("Turkana", 3.12, 35.60, "Kenya"),
        ("Dodoma", -6.16, 35.75, "Tanzania"),
        ("Dire Dawa", 9.59, 41.86, "Ethiopia"),
        ("Juba", 4.85, 31.58, "South Sudan"),
    ]

    def __init__(self, locations=None, records=None):
        self.locations = locations or self.DEFAULT_LOCATIONS
        self._injected = records

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import urllib.request

        out = []
        for name, lat, lon, country in self.locations:
            url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}"
                   f"&longitude={lon}&daily=precipitation_sum&forecast_days=7"
                   f"&timezone=auto")
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    d = json.load(r)
                precip = [p for p in d.get("daily", {}).get("precipitation_sum", []) if p is not None]
                out.append({"name": name, "lat": lat, "lon": lon,
                            "country": country, "precip": precip})
            except Exception:
                continue  # one location failing must not sink the rest
        return out

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        precip = rec.get("precip", [])
        if not precip:
            return None
        total = sum(precip)
        peak = max(precip)

        if peak >= self.FLOOD_MM:
            sev = EventSeverity.CRITICAL if peak >= 100 else EventSeverity.ALERT
            etype, note = "flood_alert", f"peak {peak:.0f}mm/day"
        elif total < self.DROUGHT_MM:
            sev = EventSeverity.ALERT if total < 1.0 else EventSeverity.WARNING
            etype, note = "drought_alert", f"7-day total {total:.1f}mm"
        else:
            return None  # normal conditions -> no event

        return CoordinationEvent(
            domain=EventDomain.WATER,
            event_type=etype,
            source=self.source,
            severity=sev,
            data={
                "title": f"{rec['name']}: {note}",
                "lat": rec.get("lat"),
                "lon": rec.get("lon"),
                "country": rec.get("country"),
                "precip_7d_mm": round(total, 1),
                "origin_feed": "open-meteo",
            },
        )
