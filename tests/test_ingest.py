"""Tests: adapters emit valid events, region filter works, pipeline runs offline."""
from africa_coord_bus.event import CoordinationEvent, EventDomain, EventSeverity
from coord_ingest import (SampleAdapter, USGSQuakeAdapter, GDACSAdapter,
                          IngestPipeline, in_east_africa)


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
