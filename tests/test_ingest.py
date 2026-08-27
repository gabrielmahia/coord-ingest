"""Tests: adapters emit valid events, region filter works, pipeline runs offline."""
from africa_coord_bus.event import CoordinationEvent, EventDomain, EventSeverity

from coord_ingest import (
    GDACSAdapter,
    IngestPipeline,
    SampleAdapter,
    USGSQuakeAdapter,
    in_east_africa,
)


def test_sample_adapter_emits_events():
    evs = SampleAdapter().to_events()
    assert len(evs) == 6
    assert all(isinstance(e, CoordinationEvent) for e in evs)


def test_region_filter_drops_global_noise():
    pipe = IngestPipeline([SampleAdapter()])
    events = pipe.collect()
    # 6 fixtures, 1 is London -> must be dropped
    assert len(events) == 5
    titles = [e.data["title"] for e in events]
    assert not any("GLOBAL NOISE" in t for t in titles)


def test_region_filter_by_country_and_bbox():
    e_ke = CoordinationEvent(domain=EventDomain.WATER, event_type="x", source="t",
                             data={"country": "Kenya"})
    e_uk = CoordinationEvent(domain=EventDomain.WATER, event_type="x", source="t",
                             data={"country": "United Kingdom", "lat": 51.5, "lon": -0.1})
    e_geo = CoordinationEvent(domain=EventDomain.WATER, event_type="x", source="t",
                              data={"lat": -1.3, "lon": 36.8})  # Nairobi, no country
    assert in_east_africa(e_ke)
    assert not in_east_africa(e_uk)
    assert in_east_africa(e_geo)


def test_usgs_adapter_maps_magnitude_to_severity():
    feature = {"properties": {"place": "near Nairobi", "mag": 6.3},
               "geometry": {"coordinates": [36.8, -1.3, 10]}}
    evs = USGSQuakeAdapter(records=[feature]).to_events()
    assert len(evs) == 1
    assert evs[0].severity == EventSeverity.ALERT  # 6.0-6.9


def test_gdacs_colour_maps_to_severity():
    rec = {"properties": {"alertlevel": "Red", "eventname": "Cyclone",
                          "latitude": -6.8, "longitude": 39.2, "country": "Tanzania"}}
    evs = GDACSAdapter(records=[rec]).to_events()
    assert evs[0].severity == EventSeverity.CRITICAL


def test_pipeline_summary_offline():
    summary = IngestPipeline([SampleAdapter()]).run()
    assert summary["collected"] == 5
    assert "water" in summary["by_domain"]
    assert summary["published"] == 0  # no bus passed


def test_pipeline_survives_a_broken_adapter():
    class Broken:
        def to_events(self): raise RuntimeError("feed down")
    summary = IngestPipeline([Broken(), SampleAdapter()]).run()
    assert summary["collected"] == 5  # good adapter still ran


# ── Real-schema regression tests (from live smoke test 2026-07-21) ──

def test_gdacs_reads_geojson_geometry():
    """GDACS puts coordinates in geometry, not properties — regression guard."""
    rec = {"properties": {"alertlevel": "Orange", "eventname": "Flood in Ethiopia",
                          "eventtype": "FL", "country": "Ethiopia"},
           "geometry": {"type": "Point", "coordinates": [37.26, 6.17]}}
    evs = GDACSAdapter(records=[rec]).to_events()
    assert evs[0].data["lat"] == 6.17 and evs[0].data["lon"] == 37.26
    assert in_east_africa(evs[0])  # must survive the region filter


def test_reliefweb_degrades_gracefully():
    """ReliefWeb v1 GET was retired (410). A dead feed must not crash the run."""
    from coord_ingest import ReliefWebAdapter
    # injected None forces the live path; if network fails it must return []
    evs = ReliefWebAdapter().to_events()
    assert isinstance(evs, list)  # never raises


# ── Open-Meteo adapter (the upstream weather-signal rail) ──

def test_openmeteo_drought_signal():
    from coord_ingest import OpenMeteoAdapter
    rec = [{"name": "Dry", "lat": -1.0, "lon": 36.0, "country": "Kenya",
            "precip": [0.1, 0, 0.2, 0, 0, 0, 0]}]
    evs = OpenMeteoAdapter(records=rec).to_events()
    assert evs[0].event_type == "drought_alert"
    assert evs[0].severity == EventSeverity.ALERT  # <1mm total


def test_openmeteo_flood_signal():
    from coord_ingest import OpenMeteoAdapter
    rec = [{"name": "Wet", "lat": -6.0, "lon": 38.0, "country": "Tanzania",
            "precip": [5, 60, 10, 2, 0, 1, 0]}]
    evs = OpenMeteoAdapter(records=rec).to_events()
    assert evs[0].event_type == "flood_alert"


def test_openmeteo_normal_emits_nothing():
    from coord_ingest import OpenMeteoAdapter
    rec = [{"name": "OK", "lat": 0.0, "lon": 34.0, "country": "Kenya",
            "precip": [8, 12, 6, 9, 7, 10, 5]}]
    assert OpenMeteoAdapter(records=rec).to_events() == []


def test_openmeteo_events_cascade():
    """A weather-derived drought must fire the real routing cascade."""
    from africa_coord_bus import KENYA_ROUTING_TABLE

    from coord_ingest import OpenMeteoAdapter
    rec = [{"name": "Turkana", "lat": 3.1, "lon": 35.6, "country": "Kenya",
            "precip": [0, 0, 0, 0, 0, 0, 0]}]
    ev = OpenMeteoAdapter(records=rec).to_events()[0]
    fired = [r.name for r in KENYA_ROUTING_TABLE if r.matches(ev)]
    assert "drought→parametric_insurance" in fired


# --- HDX adapter (OCHA Humanitarian Data Exchange, key-free CKAN API) ---------
from coord_ingest import HDXAdapter

_HDX_FIXTURES = [
    {  # crisis-tagged -> should lift to water/alert
        "name": "kenya-drought-key-figures",
        "title": "Kenya: Drought-related key figures",
        "metadata_modified": "2026-07-27T14:08:01",
        "organization": {"title": "Humanitarian partners"},
        "groups": [{"name": "ken"}],
        "tags": [{"name": "drought"}, {"name": "food security"}],
    },
    {  # plain dataset in EA -> civic/warning default
        "name": "hrp-projects-uga",
        "title": "Uganda: Response Plan projects",
        "metadata_modified": "2026-07-27T14:08:01",
        "organization": {"title": "OCHA HPC Tools"},
        "groups": [{"name": "uga"}],
        "tags": [{"name": "who is doing what and where-3w-4w-5w"}],
    },
    {  # outside East Africa -> country None -> dropped by region filter
        "name": "nigeria-health-facilities",
        "title": "Nigeria: Health facilities",
        "metadata_modified": "2026-07-20T00:00:00",
        "organization": {"title": "WHO"},
        "groups": [{"name": "nga"}],
        "tags": [{"name": "health facilities"}],
    },
]


def test_hdx_maps_country_and_url():
    evs = HDXAdapter(records=_HDX_FIXTURES).to_events()
    assert len(evs) == 3
    ke = next(e for e in evs if "Kenya" in e.data["title"])
    assert ke.data["country"] == "Kenya"
    assert ke.data["url"] == "https://data.humdata.org/dataset/kenya-drought-key-figures"
    assert ke.data["origin_feed"] == "hdx"


def test_hdx_crisis_tags_lift_domain_and_severity():
    ke = HDXAdapter(records=_HDX_FIXTURES[:1]).to_events()[0]
    assert ke.domain == EventDomain.WATER          # drought -> water
    assert ke.severity == EventSeverity.ALERT      # crisis floor, above default WARNING


def test_hdx_plain_dataset_defaults_to_civic_warning():
    uga = HDXAdapter(records=_HDX_FIXTURES[1:2]).to_events()[0]
    assert uga.domain == EventDomain.CIVIC
    assert uga.severity == EventSeverity.WARNING


def test_hdx_non_east_africa_dataset_is_dropped_by_pipeline():
    kept = IngestPipeline([HDXAdapter(records=_HDX_FIXTURES)]).collect()
    titles = [e.data["title"] for e in kept]
    assert not any("Nigeria" in t for t in titles)   # nga has no EA group -> dropped
    assert len(kept) == 2


def test_hdx_unreachable_degrades_to_empty():
    # bad injected path is not used; simulate network failure via a broken subclass
    class _Broken(HDXAdapter):
        def fetch(self):
            raise RuntimeError("network down")
    # to_events calls fetch(); pipeline swallows adapter errors -> empty, no crash
    assert IngestPipeline([_Broken()]).collect() == []


# --- Kobo/ODK field-report adapter (offline mapping tests only) ---------------
from coord_ingest import KoboAdapter

_KOBO_SUBMISSIONS = [
    {
        "title": "Borehole dry - Marsabit ward",
        "_geolocation": [2.33, 37.98],          # Marsabit, Kenya (in EA bbox)
        "_submission_time": "2026-07-27T09:00:00",
    },
    {
        "title": "Test entry - Berlin",
        "_geolocation": [52.52, 13.40],          # outside region -> dropped
        "_submission_time": "2026-07-27T09:05:00",
    },
]


def test_kobo_maps_geolocation_and_domain():
    evs = KoboAdapter(asset_uid="aXYZ", domain=EventDomain.WATER,
                      records=_KOBO_SUBMISSIONS).to_events()
    assert len(evs) == 2
    dry = evs[0]
    assert dry.domain == EventDomain.WATER
    assert dry.event_type == "field_report"
    assert dry.data["lat"] == 2.33 and dry.data["lon"] == 37.98
    assert dry.data["origin_feed"] == "kobo"


def test_kobo_region_filter_drops_out_of_area_submissions():
    kept = IngestPipeline([KoboAdapter(asset_uid="aXYZ", domain=EventDomain.WATER,
                                       records=_KOBO_SUBMISSIONS)]).collect()
    assert len(kept) == 1                        # Berlin dropped by bbox
    assert "Marsabit" in kept[0].data["title"]


def test_kobo_no_token_or_form_degrades_to_empty():
    # no injected records, no asset_uid, no token -> fetch returns [] (no crash)
    assert KoboAdapter().to_events() == []
