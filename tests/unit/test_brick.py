from __future__ import annotations

from pathlib import Path

import pytest

from iot_energy_pipeline.brick import load_brick_metadata

VALID_TTL = """\
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix unit: <http://qudt.org/vocab/unit/> .
@prefix ex: <https://example.test/> .

ex:building-a a brick:Building ;
    brick:hasPart ex:zone-a .

ex:zone-a a brick:Zone ;
    brick:hasPart ex:meter-entity .

ex:panel-a a brick:Electrical_Equipment, brick:Equipment ;
    brick:isMeteredBy ex:meter-entity .

ex:meter-entity a brick:Electrical_Meter ;
    brick:hasUnit unit:KiloW-HR ;
    ref:hasExternalReference [ ref:hasTimeseriesId "meter-a" ] .
"""


def test_brick_metadata_uses_rdf_graph_and_records_join_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(VALID_TTL, encoding="utf-8")

    metadata = load_brick_metadata(path)

    join = metadata.join_by_meter_id["meter-a"]
    assert metadata.sha256
    assert join.brick_entity_id.endswith("meter-entity")
    assert tuple(value.rsplit("/", 1)[-1] for value in join.building_ids) == (
        "building-a",
    )
    assert tuple(value.rsplit("/", 1)[-1] for value in join.zone_ids) == ("zone-a",)
    assert len(join.metered_entities) == 1
    assert join.metered_entities[0].entity_id.endswith("panel-a")
    assert {
        value.rsplit("#", 1)[-1] for value in join.metered_entities[0].type_uris
    } == {"Electrical_Equipment", "Equipment"}
    assert join.unit_uri.endswith("KiloW-HR")
    assert join.usage_type is None
    assert metadata.report.matched_meter_count == 1


def test_hkust_timeseries_filename_and_usage_type_are_preserved(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(
        """\
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix ref: <https://brickschema.org/schema/Brick/ref#> .
@prefix unit: <http://qudt.org/vocab/unit/> .
@prefix ext: <https://www.HKUST_Electric_Meter.com/schema/BrickExtension#> .
@prefix ex: <https://example.test/> .

ex:building-b a brick:Building ; brick:hasPart ex:zone-b .
ex:zone-b a brick:Zone ; brick:hasPart ex:meter-d0001 .
ex:room-a a brick:Room ; brick:isMeteredBy ex:meter-d0001 .
ex:light-a a brick:Lighting ; brick:isMeteredBy ex:meter-d0001 .
ex:meter-d0001 a brick:Electrical_Meter ;
    brick:hasUnit unit:KiloW-HR ;
    ext:usageType [ brick:value "Essential" ] ;
    ref:hasExternalReference [
        a ref:TimeseriesReference ;
        ref:hasTimeseriesData "D0001.xlsx"
    ] .
""",
        encoding="utf-8",
    )

    join = load_brick_metadata(path).join_by_meter_id["D0001"]

    assert join.usage_type == "Essential"
    assert [
        entity.entity_id.rsplit("/", 1)[-1] for entity in join.metered_entities
    ] == [
        "light-a",
        "room-a",
    ]
    assert join.building_ids[0].endswith("building-b")
    assert join.zone_ids[0].endswith("zone-b")


def test_conflicting_timeseries_identifier_forms_fail(tmp_path: Path) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(
        VALID_TTL.replace(
            'ref:hasTimeseriesId "meter-a"',
            'ref:hasTimeseriesId "meter-a" ; ref:hasTimeseriesData "meter-b.xlsx"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="conflicting Brick timeseries identifiers"):
        load_brick_metadata(path)


@pytest.mark.parametrize(
    "filename",
    ("", "D0001.csv", "folder/D0001.xlsx", r"folder\\D0001.xlsx", ".xlsx"),
)
def test_invalid_timeseries_filenames_fail(tmp_path: Path, filename: str) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(
        VALID_TTL.replace(
            'ref:hasTimeseriesId "meter-a"',
            f'ref:hasTimeseriesData "{filename}"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid Brick timeseries filename"):
        load_brick_metadata(path)


def test_multiple_buildings_and_metered_entities_remain_sorted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(
        VALID_TTL
        + """
ex:building-b a brick:Building ; brick:hasPart ex:zone-a .
ex:room-z a brick:Room ; brick:isMeteredBy ex:meter-entity .
""",
        encoding="utf-8",
    )

    join = load_brick_metadata(path).join_by_meter_id["meter-a"]

    assert [value.rsplit("/", 1)[-1] for value in join.building_ids] == [
        "building-a",
        "building-b",
    ]
    assert [value.entity_id.rsplit("/", 1)[-1] for value in join.metered_entities] == [
        "panel-a",
        "room-z",
    ]


def test_malformed_turtle_fails_with_a_deterministic_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.ttl"
    path.write_text("@prefix brick: <broken", encoding="utf-8")

    with pytest.raises(ValueError, match="malformed Brick TTL"):
        load_brick_metadata(path)


def test_multiple_entities_for_one_external_meter_fail(tmp_path: Path) -> None:
    path = tmp_path / "metadata.ttl"
    path.write_text(
        VALID_TTL
        + """
ex:second-meter a brick:Electrical_Meter ;
    ref:hasExternalReference [ ref:hasTimeseriesId "meter-a" ] .
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="multiple Brick entities"):
        load_brick_metadata(path)
