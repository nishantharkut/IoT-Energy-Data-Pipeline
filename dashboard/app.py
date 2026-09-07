"""Read-only Streamlit presentation for sealed finalized runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dashboard.data import REQUIRED_VIEWS, load_finalized_run


def _show_view(streamlit: Any, view: dict[str, Any]) -> None:
    streamlit.subheader(str(view.get("title", "View")))
    if view.get("description"):
        streamlit.caption(str(view["description"]))
    rows = view.get("rows")
    if isinstance(rows, list):
        streamlit.dataframe(rows, use_container_width=True)
    else:
        streamlit.json(view)


def main() -> None:
    import streamlit as st

    st.set_page_config(page_title="Auditable IoT Energy Pipeline", layout="wide")
    st.title("Auditable IoT Energy Data Pipeline")
    run_path = Path(
        st.sidebar.text_input(
            "Sealed finalized run directory", "reports/finalized/fixture-clean"
        )
    )
    try:
        run = load_finalized_run(run_path)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()
    st.caption(f"Run {run['replay_run_id']} — finalized, immutable, and reconciled")
    tabs = st.tabs([name.replace("_", " ").title() for name in REQUIRED_VIEWS])
    for tab, name in zip(tabs, REQUIRED_VIEWS, strict=True):
        with tab:
            _show_view(st, run["views"][name])


if __name__ == "__main__":
    main()
