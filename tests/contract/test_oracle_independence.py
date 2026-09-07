from __future__ import annotations

import ast
from pathlib import Path


def test_hadoop_mapper_and_reducer_do_not_import_spark_implementation() -> None:
    for path in (
        Path("jobs/hadoop/mapper.py"),
        Path("jobs/hadoop/reducer.py"),
        Path("jobs/hadoop/record_mapper.py"),
        Path("jobs/hadoop/record_reducer.py"),
    ):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(name.startswith("jobs.spark") for name in imports)
        assert not any(name.startswith("iot_energy_pipeline") for name in imports)


def test_hadoop_source_is_not_hidden_by_runtime_ignore_rule() -> None:
    lines = Path(".gitignore").read_text(encoding="utf-8").splitlines()
    assert "hadoop/" not in {line.strip() for line in lines}
