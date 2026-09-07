# Course results

This tracked file is a publication boundary, not an empirical result. It
remains free of measured claims until the complete designated-machine matrix
has produced `experiment-results-v1` and sealed run directories.

Generate the measured report with:

```text
python -m iot_energy_pipeline.course_report \
  --experiment-results <runtime>/experiment-results.json \
  --runtime-root <runtime> \
  --output reports/generated/course-results.md
```

The generator refuses incomplete results, altered run manifests, unverified
decision reports, and pre-existing output. The generated report labels all
monetary values as hypothetical scenarios and states that they are not HKUST
financial results.
