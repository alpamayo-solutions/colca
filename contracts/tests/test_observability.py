from colca_data_contracts import container_resource_health_metrics


def test_container_health_declarations_are_queryable_locally_and_upstream():
    metrics = container_resource_health_metrics()

    assert [metric.key for metric in metrics] == ["container_cpu", "container_memory"]
    assert all("{service_name_pattern}" in metric.query for metric in metrics)
    assert all("{node_id_pattern}" in metric.query for metric in metrics)
    assert all("{{" in metric.query and "}}" in metric.query for metric in metrics)
    assert all("(edge|hub):service_" in metric.query for metric in metrics)
    assert all("container_cpu_usage_seconds_total" not in metric.query for metric in metrics)
