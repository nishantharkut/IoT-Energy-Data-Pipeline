"""Unit tests for package structure."""

import importlib
import sys

OPTIONAL_PACKAGES = frozenset(
    {
        "confluent_kafka",
        "pyspark",
        "pyarrow",
        "openpyxl",
        "rdflib",
        "streamlit",
        "plotly",
    }
)


def _fresh_remove():
    for k in list(sys.modules):
        if k == "iot_energy_pipeline" or k.startswith("iot_energy_pipeline."):
            del sys.modules[k]


def test_import_package():
    _fresh_remove()
    import iot_energy_pipeline

    assert iot_energy_pipeline is not None


def test_package_version():
    _fresh_remove()
    import iot_energy_pipeline

    assert iot_energy_pipeline.__version__ == "0.1.0"


def test_no_optional_imports_at_load():
    _fresh_remove()
    before = set(sys.modules)
    importlib.import_module("iot_energy_pipeline")
    after = set(sys.modules)
    new = after - before
    for pkg in OPTIONAL_PACKAGES:
        for m in new:
            if m == pkg or m.startswith(pkg + "."):
                raise AssertionError(f"Optional {pkg} loaded as {m}")
