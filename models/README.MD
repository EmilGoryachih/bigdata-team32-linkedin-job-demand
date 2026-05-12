# `models/` — Spark ML model storage

**This folder is intentionally empty in the repository.**

The four trained Spark MLlib `PipelineModel`s produced by Stage 3 live
on HDFS, not on the local filesystem:

```
/user/team32/linkedin/ml/models/rf      best Random Forest          (numTrees, maxDepth, minInstancesPerNode tuned)
/user/team32/linkedin/ml/models/gbt     best Gradient Boosted Trees (stepSize, maxDepth, minInstancesPerNode tuned)
/user/team32/linkedin/ml/models/svm     best Linear SVC             (regParam, threshold, aggregationDepth tuned)
/user/team32/linkedin/ml/models/nb      best Naive Bayes            (smoothing, modelType, thresholds tuned)
```

The soft-voting ensemble is **not** persisted as a separate model — it
is recomputed on the fly from the three saved member models (rf, gbt,
svm) whenever predictions are needed. See
[`scripts/stage3_predict_samples.py`](../scripts/stage3_predict_samples.py)
for the reference implementation.

## Why on HDFS instead of here

1. **Distributed-correctness.** The project rubric requires training on
   the YARN cluster (with an 80 % penalty for local training). Models
   trained on the cluster are naturally stored on the cluster — HDFS —
   so any executor can load them without copying through a single
   gateway machine.
2. **Size.** A tuned RF with 200 trees at maxDepth 15 is hundreds of
   megabytes spread across many files. The four models together are
   close to 1 GB. That does not belong in a Git repo and is gitignored
   anyway (see `.gitignore`).
3. **Not single files.** A Spark `PipelineModel` is a directory tree
   containing pipeline metadata, per-stage parameters, and the
   underlying model objects — not a serialisable artefact like a
   scikit-learn `.pkl`.

## Reproducing the models

The models are recreated deterministically from the seed-controlled
training script — `seed=42` everywhere (CrossValidator, RF, GBT,
`F.rand`). Re-running:

```bash
bash scripts/stage3.sh
```

will produce numerically identical models (up to Spark's parallel-sum
non-associativity in the 5th–6th decimal) and overwrite the HDFS paths
listed above.

## Loading a model in your own PySpark script

```python
from pyspark.ml import PipelineModel
gbt = PipelineModel.load("/user/team32/linkedin/ml/models/gbt")
predictions = gbt.transform(new_data_dataframe)
```

For the soft-voting ensemble, see the helper functions
`_positive_class_prob`, `build_ensemble`, and `evaluate_ensemble` in
[`scripts/stage3_train_models.py`](../scripts/stage3_train_models.py).
