# Output artifacts

Files produced by the pipeline. The Stage 3 (Spark ML) outputs are the
ones most likely to be inspected by a grader.

## Stage 2 — Hive

| File | Description |
|---|---|
| `stage2_hive_counts.tsv` | Row counts of every Hive table created by stage 2. |

## Stage 3 — Spark ML (demand classification)

### Dataset preparation (3.1)

| File | Description |
|---|---|
| `stage3_prepare_summary.txt` | Aggregation key, small-group filter stats, quantile threshold, train/test row counts (70/30 split), split seed, HDFS dataset paths. |
| `stage3_class_distribution_train.csv` | Train-set class balance for `high_demand`. |
| `stage3_class_distribution_test.csv` | Test-set class balance for `high_demand`. |

### Model training & evaluation (3.2)

| File | Description |
|---|---|
| `stage3_model_metrics.csv` | One row per model (rf, gbt, svm, nb, ensemble) with columns `accuracy, f1, auc_roc, auc_pr` on the held-out test set. |
| `stage3_rf_best_params.txt` | Random Forest hyperparameters chosen by 3-fold CV (27-combo grid). |
| `stage3_gbt_best_params.txt` | Gradient Boosted Trees hyperparameters chosen by 3-fold CV (27-combo grid). |
| `stage3_svm_best_params.txt` | Linear SVC hyperparameters chosen by 3-fold CV (27-combo grid). |
| `stage3_nb_best_params.txt` | Naive Bayes hyperparameters chosen by 3-fold CV (27-combo grid). |
| `stage3_<model>_cv_grid.csv` | Full CV grid for each model: one row per hyperparameter combination, sorted by mean validation F1. Documents the tuning process. List-valued params (e.g. NB `thresholds`) are pipe-separated to stay one CSV cell wide. |
| `stage3_rf_feature_importance.csv` | Per-feature importance from the best Random Forest, sorted descending. |
| `stage3_gbt_feature_importance.csv` | Per-feature importance from the best Gradient Boosted Trees, sorted descending. |
| `stage3_ensemble_info.txt` | Members, voting strategy, and decision threshold for the soft-voting ensemble. |
| `stage3_rf_confusion.csv` | Confusion matrix counts (Random Forest). |
| `stage3_gbt_confusion.csv` | Confusion matrix counts (Gradient Boosted Trees). |
| `stage3_svm_confusion.csv` | Confusion matrix counts (Linear SVC). |
| `stage3_nb_confusion.csv` | Confusion matrix counts (Naive Bayes). |
| `stage3_ensemble_confusion.csv` | Confusion matrix counts (soft-voting ensemble of rf + gbt + svm). |

### Sample predictions (3.3)

| File | Description |
|---|---|
| `stage3_sample_predictions.csv` | Predictions from all four models plus the soft-voting ensemble (`pred_ensemble`) on a held-out test sample and a few hand-crafted (country, position) scenarios. |

## HDFS-only artifacts (not in this folder)

These live on HDFS because they are too large for the repo, but they are
the inputs for the dashboard stage and reproducible from the pipeline.

```
/user/team32/linkedin/ml/dataset_train          aggregated training Parquet
/user/team32/linkedin/ml/dataset_test           aggregated held-out Parquet
/user/team32/linkedin/ml/models/rf              best Random Forest PipelineModel
/user/team32/linkedin/ml/models/gbt             best Gradient Boosted Trees PipelineModel
/user/team32/linkedin/ml/models/svm             best Linear SVC PipelineModel
/user/team32/linkedin/ml/models/nb              best Naive Bayes PipelineModel
```

The ensemble is not persisted as a separate Spark model — it is computed
on the fly from the three saved member models (rf, gbt, svm) by averaging
their per-row P(class=1) and thresholding at 0.5.
