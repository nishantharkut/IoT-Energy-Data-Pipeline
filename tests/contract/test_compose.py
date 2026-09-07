from __future__ import annotations

import json
import subprocess
from pathlib import Path


def _compose() -> dict:
    result = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_compose_declares_actual_kafka_spark_hdfs_and_hadoop_services() -> None:
    config = _compose()

    assert set(config["services"]) >= {
        "kafka",
        "spark",
        "spark-client",
        "hdfs",
        "hadoop",
    }
    assert config["networks"]["pipeline"]["driver"] == "bridge"
    assert set(config["volumes"]) >= {
        "kafka-data",
        "hdfs-name",
        "hdfs-data",
        "hadoop-logs",
    }


def test_distributed_services_are_pinned_healthy_and_resource_bounded() -> None:
    services = _compose()["services"]

    for name in ("kafka", "spark", "hdfs", "hadoop"):
        service = services[name]
        image = service.get("image", "")
        assert image and ("@sha256:" in image or ":" in image)
        assert not image.endswith(":latest")
        assert service.get("healthcheck", {}).get("test")
        assert service.get("networks") == {"pipeline": None}
        limits = service.get("deploy", {}).get("resources", {}).get("limits", {})
        assert limits.get("cpus")
        assert limits.get("memory")


def test_kafka_uses_explicit_single_node_kraft_and_acknowledged_topic_config() -> None:
    kafka = _compose()["services"]["kafka"]
    environment = kafka["environment"]

    assert environment["KAFKA_PROCESS_ROLES"] == "broker,controller"
    assert environment["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] == "1"
    assert environment["KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR"] == "1"
    assert environment["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] == "false"
    assert "kafka:9092" in environment["KAFKA_ADVERTISED_LISTENERS"]
    assert kafka["ports"]


def test_spark_healthcheck_targets_the_bound_master_interface() -> None:
    spark = _compose()["services"]["spark"]
    health_command = " ".join(spark["healthcheck"]["test"])

    assert '"spark",7077' in health_command
    assert '"localhost",7077' not in health_command

    worker = _compose()["services"]["spark-worker"]
    worker_health_command = " ".join(worker["healthcheck"]["test"])
    assert '"spark-worker",8081' in worker_health_command
    assert "jps" not in worker_health_command

    client = _compose()["services"]["spark-client"]
    assert client["hostname"] == "spark-client"
    assert client["image"] == spark["image"]
    assert client["hostname"] != spark["hostname"]


def test_custom_images_exist_and_never_copy_source_data() -> None:
    expected = [
        Path("infra/spark/Dockerfile"),
        Path("infra/hadoop/Dockerfile"),
        Path("infra/hadoop/hadoop-entrypoint.sh"),
        Path("infra/hadoop/conf/core-site.xml"),
        Path("infra/hadoop/conf/hdfs-site.xml"),
        Path("infra/hadoop/conf/mapred-site.xml"),
        Path("infra/hadoop/conf/yarn-site.xml"),
    ]
    for path in expected:
        assert path.is_file(), f"missing {path}"
    dockerfiles = "\n".join(
        path.read_text(encoding="utf-8") for path in expected[:2]
    ).lower()
    assert "data/raw" not in dockerfiles
    assert "datasets/" not in dockerfiles
    assert "all_data.zip" not in dockerfiles
    hadoop_dockerfile = Path("infra/hadoop/Dockerfile").read_text(encoding="utf-8")
    assert "record_mapper.py" in hadoop_dockerfile
    assert "record_reducer.py" in hadoop_dockerfile


def test_spark_image_pins_kafka_connector_for_offline_runtime() -> None:
    dockerfile = Path("infra/spark/Dockerfile").read_text(encoding="utf-8")
    artifacts = {
        "spark-sql-kafka-0-10_2.12-3.5.1.jar": (
            "cb96af6c95c0beea714f73e5bc297f962bdd82fe492d8df94239c23e6a52bcf6"
        ),
        "spark-token-provider-kafka-0-10_2.12-3.5.1.jar": (
            "b753422d64544703919abded8cbbc2ed76098da74e6b194d3407233eefc822a0"
        ),
        "kafka-clients-3.4.1.jar": (
            "9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6"
        ),
        "commons-pool2-2.11.1.jar": (
            "ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83"
        ),
        "jsr305-3.0.0.jar": (
            "bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a"
        ),
    }

    for filename, digest in artifacts.items():
        assert filename in dockerfile
        assert digest in dockerfile
    assert "sha256sum -c" in dockerfile
