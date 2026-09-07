"""Evidence-preserving joins over registered Brick Turtle metadata."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

BRICK = "https://brickschema.org/schema/Brick#"
BRICK_REF = "https://brickschema.org/schema/Brick/ref#"
HKUST_EXTENSION = "https://www.HKUST_Electric_Meter.com/schema/BrickExtension#"


@dataclass(frozen=True)
class MeteredEntity:
    """One graph entity whose consumption is attributed to a meter."""

    entity_id: str
    type_uris: tuple[str, ...]


@dataclass(frozen=True)
class BrickJoin:
    """Lossless, deterministic projection of the HKUST meter relationships."""

    brick_entity_id: str
    building_ids: tuple[str, ...]
    zone_ids: tuple[str, ...]
    metered_entities: tuple[MeteredEntity, ...]
    unit_uri: str | None
    usage_type: str | None


@dataclass(frozen=True)
class BrickJoinReport:
    matched_meter_count: int
    brick_entity_count: int
    unmatched_meter_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class BrickMetadata:
    sha256: str
    detected_brick_namespace: str | None
    join_by_meter_id: Mapping[str, BrickJoin]
    report: BrickJoinReport


def _single_text(values: set[str], label: str, subject: object) -> str | None:
    if len(values) > 1:
        raise ValueError(f"contradictory {label} assignments for {subject}")
    return next(iter(values), None)


def _timeseries_id(predicate: object, value: object) -> str:
    predicate_uri = str(predicate)
    raw = str(value).strip()
    if predicate_uri == BRICK_REF + "hasTimeseriesId":
        if not raw or any(character in raw for character in "\t\r\n"):
            raise ValueError("empty Brick timeseries identifier")
        return raw
    if predicate_uri != BRICK_REF + "hasTimeseriesData":
        raise AssertionError("unrecognized timeseries predicate")
    if (
        not raw
        or "/" in raw
        or "\\" in raw
        or not raw.endswith(".xlsx")
        or not raw.removesuffix(".xlsx")
    ):
        raise ValueError(f"invalid Brick timeseries filename: {raw!r}")
    return raw.removesuffix(".xlsx")


def _usage_type(graph: object, entity: object) -> str | None:
    usage_nodes = {
        value
        for predicate, value in graph.predicate_objects(entity)  # type: ignore[attr-defined]
        if str(predicate) == HKUST_EXTENSION + "usageType"
    }
    values: set[str] = set()
    for node in usage_nodes:
        nested = {
            str(value).strip()
            for predicate, value in graph.predicate_objects(node)  # type: ignore[attr-defined]
            if str(predicate) == BRICK + "value"
        }
        if not nested:
            # Support a direct literal while rejecting unresolved graph nodes.
            if node.__class__.__name__ == "Literal":
                nested.add(str(node).strip())
            else:
                raise ValueError(f"usageType has no Brick value for {entity}")
        values.update(nested)
    if "" in values:
        raise ValueError(f"empty usageType assignment for {entity}")
    return _single_text(values, "usage type", entity)


def load_brick_metadata(path: Path) -> BrickMetadata:
    """Parse Turtle and map each external meter ID to exactly one Brick entity."""

    try:
        from rdflib import RDF, Graph, URIRef
        from rdflib.term import Node
    except ImportError as exc:  # pragma: no cover - dependency gate
        raise RuntimeError("dataset extra is required for Brick parsing") from exc

    graph = Graph()
    try:
        graph.parse(path, format="turtle")
    except Exception as exc:
        raise ValueError(f"malformed Brick TTL: {path.name}") from exc

    namespaces = {
        str(uri)
        for prefix, uri in graph.namespace_manager.namespaces()
        if prefix == "brick" or "brickschema.org/schema/Brick#" in str(uri)
    }
    detected = sorted(namespaces)[0] if namespaces else None

    recognized_predicates = {
        URIRef(BRICK_REF + "hasTimeseriesId"),
        URIRef(BRICK_REF + "hasTimeseriesData"),
    }
    ids_by_external_node: dict[Node, set[str]] = {}
    for predicate in recognized_predicates:
        for external_node, value in graph.subject_objects(predicate):
            ids_by_external_node.setdefault(external_node, set()).add(
                _timeseries_id(predicate, value)
            )

    entities_by_meter: dict[str, set[Node]] = {}
    has_external_reference = URIRef(BRICK_REF + "hasExternalReference")
    for external_node, meter_ids in ids_by_external_node.items():
        if len(meter_ids) != 1:
            raise ValueError(
                f"conflicting Brick timeseries identifiers for {external_node}"
            )
        owners = set(graph.subjects(has_external_reference, external_node))
        if len(owners) != 1:
            raise ValueError(
                "Brick timeseries reference must have exactly one owning entity"
            )
        meter_id = next(iter(meter_ids))
        entities_by_meter.setdefault(meter_id, set()).update(owners)

    has_part = URIRef(BRICK + "hasPart")
    is_metered_by = URIRef(BRICK + "isMeteredBy")
    has_unit = URIRef(BRICK + "hasUnit")
    zone_type = URIRef(BRICK + "Zone")
    building_type = URIRef(BRICK + "Building")
    joins: dict[str, BrickJoin] = {}
    for meter_id in sorted(entities_by_meter):
        entities = entities_by_meter[meter_id]
        if len(entities) != 1:
            raise ValueError(f"multiple Brick entities for meter {meter_id!r}")
        entity = next(iter(entities))
        zones = {
            zone
            for zone in graph.subjects(has_part, entity)
            if (zone, RDF.type, zone_type) in graph
        }
        buildings = {
            building
            for zone in zones
            for building in graph.subjects(has_part, zone)
            if (building, RDF.type, building_type) in graph
        }
        metered_entities = set(graph.subjects(is_metered_by, entity))
        joins[meter_id] = BrickJoin(
            brick_entity_id=str(entity),
            building_ids=tuple(sorted(str(value) for value in buildings)),
            zone_ids=tuple(sorted(str(value) for value in zones)),
            metered_entities=tuple(
                MeteredEntity(
                    entity_id=str(metered),
                    type_uris=tuple(
                        sorted(str(value) for value in graph.objects(metered, RDF.type))
                    ),
                )
                for metered in sorted(metered_entities, key=str)
            ),
            unit_uri=_single_text(
                {str(value) for value in graph.objects(entity, has_unit)},
                "unit",
                entity,
            ),
            usage_type=_usage_type(graph, entity),
        )

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    immutable_joins: Mapping[str, BrickJoin] = MappingProxyType(joins)
    return BrickMetadata(
        sha256=digest,
        detected_brick_namespace=detected,
        join_by_meter_id=immutable_joins,
        report=BrickJoinReport(
            matched_meter_count=len(joins),
            brick_entity_count=len({join.brick_entity_id for join in joins.values()}),
        ),
    )


def load_meter_joins(path: Path) -> dict[str, dict[str, Any]]:
    """Return a plain serializable projection for non-dataclass callers."""

    metadata = load_brick_metadata(path)
    return {
        meter_id: {
            "brick_entity_id": join.brick_entity_id,
            "building_ids": list(join.building_ids),
            "zone_ids": list(join.zone_ids),
            "metered_entities": [
                {
                    "entity_id": entity.entity_id,
                    "type_uris": list(entity.type_uris),
                }
                for entity in join.metered_entities
            ],
            "unit_uri": join.unit_uri,
            "usage_type": join.usage_type,
        }
        for meter_id, join in metadata.join_by_meter_id.items()
    }
