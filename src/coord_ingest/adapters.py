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


class HDXAdapter(FeedAdapter):
    """Humanitarian Data Exchange (HDX) — OCHA's open dataset registry, served
    over a public, key-free CKAN API. This adapter surfaces *newly published or
    updated* humanitarian datasets for East Africa as coordination signals: a
    data-availability event ("IOM published new drought-displacement figures for
    Kenya") telling the bus that fresh ground-truth exists to pull into
    advisories, water testing, or cross-border coordination.

    Unlike the quake/rainfall adapters (point-in-time physical signals), HDX
    events mark *information availability*, so default severity is WARNING and
    lifts only when crisis keywords (drought, flood, cholera, outbreak,
    displacement) appear in the title or tags. The bus decides what to do with
    the signal; this adapter only normalizes it.

    HDX 'groups' are ISO3 country slugs, so region membership is exact here — no
    bounding-box guessing. Datasets outside the East-Africa set carry country
    ``None`` and are dropped by the pipeline's region filter.
    """

    domain = EventDomain.CIVIC
    event_type = "humanitarian_dataset"
    source = "coord-ingest.hdx"

    EAST_AFRICA_ISO3 = {
        "ken": "Kenya", "tza": "Tanzania", "eth": "Ethiopia", "uga": "Uganda",
        "ssd": "South Sudan", "som": "Somalia", "rwa": "Rwanda", "bdi": "Burundi",
    }
    # crisis keyword -> (domain, severity floor); most specific signal wins
    _CRISIS = {
        "drought": (EventDomain.WATER, EventSeverity.ALERT),
        "flood": (EventDomain.WATER, EventSeverity.ALERT),
        "cholera": (EventDomain.HEALTH, EventSeverity.ALERT),
        "outbreak": (EventDomain.HEALTH, EventSeverity.ALERT),
        "displacement": (EventDomain.CIVIC, EventSeverity.WARNING),
        "food security": (EventDomain.AGRICULTURE, EventSeverity.WARNING),
        "nutrition": (EventDomain.HEALTH, EventSeverity.WARNING),
    }
    API = "https://data.humdata.org/api/3/action/package_search"
    _ORDER = list(EventSeverity)  # info < warning < alert < critical (definition order)

    def __init__(self, records: list[dict] | None = None, rows: int = 25):
        # inject records to test without network; rows caps live query size
        self._injected = records
        self.rows = rows

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import urllib.parse
        import urllib.request

        fq = "groups:(" + " OR ".join(self.EAST_AFRICA_ISO3) + ")"
        qs = urllib.parse.urlencode(
            {"fq": fq, "sort": "metadata_modified desc", "rows": self.rows}
        )
        try:
            with urllib.request.urlopen(f"{self.API}?{qs}", timeout=20) as r:
                return json.load(r).get("result", {}).get("results", [])
        except Exception:
            return []  # HDX unreachable -> pipeline runs on its other adapters

    def _classify(self, rec: dict) -> tuple[EventDomain, EventSeverity]:
        haystack = (
            (rec.get("title", "") or "") + " "
            + " ".join(t.get("name", "") for t in rec.get("tags", []))
        ).lower()
        # When several crisis keywords match, the highest-severity signal owns
        # the domain (ties broken by definition order). This keeps a drought +
        # food-security dataset classified as the more acute water signal rather
        # than whichever keyword happened to be checked last.
        domain, sev = self.domain, EventSeverity.WARNING
        best_rank = -1
        for kw, (dom, floor) in self._CRISIS.items():
            if kw in haystack and self._ORDER.index(floor) > best_rank:
                best_rank = self._ORDER.index(floor)
                domain, sev = dom, floor
        return domain, sev

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        groups = [g.get("name") for g in rec.get("groups", [])]
        country = next(
            (self.EAST_AFRICA_ISO3[g] for g in groups if g in self.EAST_AFRICA_ISO3),
            None,
        )
        domain, sev = self._classify(rec)
        return CoordinationEvent(
            domain=domain,
            event_type=self.event_type,
            source=self.source,
            severity=sev,
            data={
                "title": rec.get("title", ""),
                "organization": (rec.get("organization") or {}).get("title"),
                "country": country,
                "updated": rec.get("metadata_modified"),
                "url": f"https://data.humdata.org/dataset/{rec.get('name', '')}",
                "tags": [t.get("name") for t in rec.get("tags", [])][:8],
                "origin_feed": "hdx",
            },
        )


class KoboAdapter(FeedAdapter):
    """KoboToolbox / ODK field submissions -> CoordinationEvents.

    The other adapters carry *top-down* signals (agency datasets, satellites,
    seismographs). This one carries *bottom-up* ground truth: a community health
    volunteer logging a cholera case, a water committee reporting a dry borehole,
    an extension officer flagging armyworm. KoboToolbox (and ODK Central) are the
    dominant field-data tools across East African NGOs and ministries, so this is
    the rail that lets citizen/field reports cascade on the same bus as the
    automated feeds.

    Config, because form schemas vary:
      - ``asset_uid``  : the Kobo asset/form id
      - ``domain``     : the bus domain this form maps to (a water-point form is
                         WATER; a clinic form is HEALTH)
      - ``title_field``: submission field used as the human-readable title
      - ``country_field`` (optional): field naming the country; else geo is used
    Geolocation is read from Kobo's ``_geolocation`` ([lat, lon]); the pipeline's
    bounding-box filter keeps it region-scoped.

    NOTE ON VERIFICATION: the mapping logic is unit-tested with injected
    submissions. The live network path (auth'd GET against a real form) is NOT
    smoke-tested in this package because it requires a private token + a live
    form. Verify against your own Kobo/ODK server before operational use.
    """

    event_type = "field_report"
    source = "coord-ingest.kobo"

    def __init__(
        self,
        asset_uid: str = "",
        domain: EventDomain = EventDomain.CIVIC,
        *,
        title_field: str = "title",
        country_field: str | None = None,
        base_url: str = "https://kf.kobotoolbox.org",
        token_env: str = "KOBO_API_TOKEN",
        records: list[dict] | None = None,
    ):
        self.asset_uid = asset_uid
        self.domain = domain
        self.title_field = title_field
        self.country_field = country_field
        self.base_url = base_url.rstrip("/")
        self.token_env = token_env
        self._injected = records

    def fetch(self) -> list[dict]:
        if self._injected is not None:
            return self._injected
        import os
        import urllib.request

        token = os.environ.get(self.token_env)
        if not token or not self.asset_uid:
            return []  # no creds/form -> nothing to ingest, don't crash the run
        url = f"{self.base_url}/api/v2/assets/{self.asset_uid}/data/?format=json"
        req = urllib.request.Request(url, headers={"Authorization": f"Token {token}"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.load(r).get("results", [])
        except Exception:
            return []  # unreachable/expired token -> degrade, run other adapters

    def _record_to_event(self, rec: dict) -> CoordinationEvent | None:
        geo = rec.get("_geolocation") or [None, None]
        lat, lon = (geo + [None, None])[:2] if isinstance(geo, list) else (None, None)
        country = rec.get(self.country_field) if self.country_field else None
        return CoordinationEvent(
            domain=self.domain,
            event_type=self.event_type,
            source=self.source,
            severity=EventSeverity.WARNING,
            data={
                "title": rec.get(self.title_field, ""),
                "lat": lat,
                "lon": lon,
                "country": country,
                "submitted": rec.get("_submission_time"),
                "asset_uid": self.asset_uid,
                "origin_feed": "kobo",
            },
        )
