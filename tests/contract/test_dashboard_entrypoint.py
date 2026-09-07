from __future__ import annotations

import runpy


def test_streamlit_dashboard_file_imports_without_package_context() -> None:
    namespace = runpy.run_path("dashboard/app.py", run_name="dashboard_probe")

    assert callable(namespace["main"])
