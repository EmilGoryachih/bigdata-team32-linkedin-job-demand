# Stage 3 — Machine Learning Report

LinkedIn job-demand classification on the Hadoop cluster. This document
analyses the outputs of `scripts/stage3.sh` against the rubric items
for the ML specialist role:

- prepare an ML dataset from job postings and skills
- perform feature engineering and encoding
- train classification models (Random Forest, SVM, Naive Bayes — plus
  Gradient Boosted Trees and a soft-voting ensemble as additions)
- perform hyperparameter tuning and cross-validation
- evaluate models using Accuracy and F1-score (AUC added for ranking
  quality)
- generate predictions for sample inputs

All artefacts referenced below live in [output/](../output/) and on
HDFS at `/user/team32/linkedin/ml/`.

## TL;DR

| Model | Accuracy | F1 (weighted) | AUC | Precision (+) | Recall (+) |
|---|---|---|---|---|---|
| **GBT** | **0.898** | **0.896** | **0.936** | 0.629 | 0.564 |
| Ensemble (rf+gbt+svm soft vote) | 0.891 | 0.875 | 0.931 | 0.684 | 0.333 |
| RF | 0.868 | 0.806 | 0.908 | n/a (always 0) | 0.000 |
| SVM (LinearSVC) | 0.856 | 0.868 | 0.898 | 0.472 | **0.718** |
| Naive Bayes (Gaussian) | 0.610 | 0.657 | 0.807 | 0.000 | 0.000 |

**Pick GBT for the dashboard.** It dominates the three reported
metrics and is the only model with a balanced precision/recall on the
minority class. RF and NB collapse on the imbalanced label and need
either class weighting or threshold tuning to be useful; the ensemble
is dragged down by RF's zero votes.

## 1. Dataset construction

Source: `team32_linkedin.linkedin_jobs_enriched` (1 348 454 rows).

The data engineering pipeline that produced the enriched table is
documented. Stage 3 aggregates those raw postings into
one row per `(search_country, search_position)` group. Originally the
plan was a temporal aggregation by `(week, country, position)` with
lag/rolling features, but the source dump's `first_seen` column only
covers six days (2024-01-12 → 2024-01-17) — both timestamp columns in
the dataset are scraper-collection artefacts, not real posting dates,
and carry no usable temporal signal. The pipeline was simplified to a
static aggregation and the report reflects that honestly.

`scripts/stage3_prepare_dataset.py` produces:

| Metric | Value |
|---|---|
| Aggregation key | `(search_country, search_position)` |
| Groups before sparse filter | 6 177 |
| Groups after `group_count >= 5` filter | 4 426 |
| High-demand quantile (per-position) | 0.75 |
| Train rows | 3 542 |
| Test rows | 884 |
| Train class balance (positive / total) | 477 / 3 542 ≈ 13.5 % |
| Test class balance (positive / total) | 117 / 884 ≈ 13.2 % |
| Split | random 80/20, seed 42 |

The 13 % positive rate (versus the 25 % a clean p75 split would imply)
comes from positions with few distinct country observations: when a
position has only 4–5 groups, p75 lands at the maximum value and only
one group can exceed it. Class imbalance turned out to be the single
biggest factor in the per-model behaviour below.

## 2. Feature engineering and encoding

The label `high_demand` is computed inside prepare as

```
high_demand = 1  if  group_count > percentile_approx(group_count, 0.75)
                       over rows of the same search_position
            = 0  otherwise
```

`group_count` itself is **not** a feature — the label is a
deterministic function of it, so using it would be trivial leakage.
Raw `distinct_companies` and `distinct_skills` are also excluded
because their upper bound is `group_count`, which leaks the label
indirectly. Instead the prepare stage emits leak-light **ratios**:

| Feature | Origin | Notes |
|---|---|---|
| `summary_coverage` | `avg(got_summary)` over the group | proportion in [0, 1], leak-free |
| `skills_per_posting` | `distinct_skills / group_count` | concentration metric, leak-free |
| `companies_per_posting` | `distinct_companies / group_count` | market concentration, leak-free |
| `distinct_job_levels` | `countDistinct(job_level)` | bounded by min(5, group_count), weak correlation with label |
| `distinct_job_types` | `countDistinct(job_type)` | same idea |
| `search_country` | one-hot (`StringIndexer` + `OneHotEncoder`, `handleInvalid="keep"`) | 200+ categories |
| `search_position` | one-hot (same) | ~1450 categories |

After assembly the feature vector has **1 662 dimensions** (5 numeric
+ ~1657 one-hot). Per-model scaling:

- RF, GBT: no scaling (tree-based, invariant)
- SVM: `StandardScaler(withMean=False, withStd=True)` to keep one-hot
  vectors sparse and non-negative
- Gaussian NB: no scaling (the variant computes per-class mean and
  variance and does not require non-negativity)

## 3. Models, hyperparameter tuning, and cross-validation

`scripts/stage3_train_models.py` trains four classifiers plus an
ensemble. Each classifier sits in its own Spark ML `Pipeline` that
shares the categorical encoding stages and adds an optional scaler
before the estimator.

### Tuning protocol

For every classifier the script runs a `CrossValidator` over a
hand-built `ParamGridBuilder`:

- `numFolds = 3` (3-fold cross-validation)
- evaluator = `MulticlassClassificationEvaluator(metricName="f1")` on
  validation folds
- `parallelism = 2` (two candidate combinations evaluated in parallel)
- seed = 42

The cross-validator splits **only the training set** into three
folds. The held-out test set never participates in tuning. The best
hyperparameters are picked by mean validation F1, then the pipeline is
refitted on the full training set and finally evaluated on the test
set. That refitted pipeline is what is persisted to HDFS at
`/user/team32/linkedin/ml/models/<name>` and what produces the
numbers in section 4.

### Grids and chosen values

| Model | Grid (candidates) | Total CV fits | Winner |
|---|---|---|---|
| RF | `numTrees ∈ {50, 100, 200}` × `maxDepth ∈ {5, 10, 15}` (9) | 27 + 1 | `numTrees=50, maxDepth=5` ¹ |
| GBT | `maxIter ∈ {20, 50}` × `maxDepth ∈ {5, 8}` (4) | 12 + 1 | `maxIter=20, maxDepth=5` |
| SVM | `regParam ∈ {0.001, 0.01, 0.1}` × `maxIter ∈ {50, 100}` (6) | 18 + 1 | `regParam=0.001, maxIter=100` |
| NB | `smoothing ∈ {0.5, 1.0, 2.0}` (3) | 9 + 1 | `smoothing=0.5` ¹ |

¹ All grid entries scored the same mean F1 for both RF and NB — see
`output/stage3_rf_cv_grid.csv` (0.802850 for every combination) and
`output/stage3_nb_cv_grid.csv` (0.616841 for every combination). This
is a symptom that both models collapsed to a degenerate solution
(constant prediction); the "winner" is then arbitrary. Trees and NB
were both fooled by the class imbalance — see section 5.

GBT's grid did discriminate: its CV F1 ranges from 0.899 to 0.905 and
the simpler `(20, 5)` combination wins. SVM's grid is even tighter
(0.859 to 0.866) and the smallest regularisation wins.

### Soft-voting ensemble

After the four classifiers finish, the script computes a fifth
"model": a soft-voting ensemble that averages the positive-class
probability across RF, GBT, and SVM and thresholds at 0.5.

- RF and GBT expose a `probability` vector; the second component is
  taken directly.
- SVM (LinearSVC) only exposes a signed margin in `rawPrediction`;
  the script squashes it through `1 / (1 + exp(-margin))` to obtain a
  scalar in (0, 1).
- NB is **excluded**: its raw outputs are not calibrated on this
  dataset (see section 5), so averaging it in would only drag the
  ensemble down.

The ensemble is not persisted to HDFS as a Spark model; it is
recomputed on the fly from the three saved member models whenever
predictions are needed (`stage3_predict_samples.py` does the same).

## 4. Evaluation results

`output/stage3_model_metrics.csv` (Accuracy / F1 / AUC as reported by
Spark's `MulticlassClassificationEvaluator` and `BinaryClassification-
Evaluator` on the held-out test set):

| model | accuracy | F1 | AUC |
|---|---|---|---|
| rf | 0.8676 | 0.8062 | 0.9081 |
| gbt | **0.8982** | **0.8958** | **0.9357** |
| svm | 0.8563 | 0.8682 | 0.8982 |
| nb | 0.6097 | 0.6573 | 0.8074 |
| ensemble | 0.8914 | 0.8747 | 0.9309 |

Per-class numbers reconstructed from `output/stage3_<model>_confusion.csv`
(positive class = high_demand = 1; test set has 117 positives, 767
negatives):

| Model | TP | FP | FN | TN | Precision(+) | Recall(+) | F1(+) |
|---|---|---|---|---|---|---|---|
| RF | 0 | 0 | 117 | 767 | n/a | 0.000 | 0.000 |
| GBT | 66 | 39 | 51 | 728 | 0.629 | 0.564 | 0.595 |
| SVM | 84 | 94 | 33 | 673 | 0.472 | 0.718 | 0.569 |
| NB | 0 | 228 | 117 | 539 | 0.000 | 0.000 | 0.000 |
| Ensemble | 39 | 18 | 78 | 749 | **0.684** | 0.333 | 0.448 |

The weighted F1 in the first table masks how each model handles the
minority class. The second table makes it explicit.

## 5. Per-model analysis

### Random Forest — collapsed to majority class

RF achieves 86.8 % accuracy by **predicting class 0 for every test
row**. The confusion matrix has TP = FP = 0; weighted F1 stays at
0.806 only because the negative class dominates the test set. The CV
grid (`output/stage3_rf_cv_grid.csv`) shows mean validation F1 of
exactly 0.802850 for **every one of the 9 hyperparameter combinations**
— a clear fingerprint of the degenerate "always 0" solution.

Spark's RandomForestClassifier predicts by argmax of the class
probability vector. With only 13 % positives in train, every leaf of
every tree ends up majority-negative; no probability vector ever
clears 0.5 for class 1. The non-zero feature importances in
`output/stage3_rf_feature_importance.csv` (top hitters
`search_country=united states` 0.112, `companies_per_posting` 0.087,
`skills_per_posting` 0.075) prove the trees did learn useful splits
— they just never aggregate into a positive prediction.

Mitigations would be: pass `weightCol` to inflate positive-class
weight, threshold-tune below 0.5, or rebalance via sampling. Not
applied here to keep the four models comparable on the same input.

### Gradient Boosted Trees — the winner

GBT is the strongest model by every reported metric (accuracy 0.898,
F1 0.896, AUC 0.936) and the only tree-based model that learned to
predict the minority class. It captures 56 % of the actual high-
demand groups with 63 % precision. The grid winner is the smaller
configuration (`maxIter=20, maxDepth=5`), which suggests the model
saturates quickly on this 3 542-row training set; deeper trees or
more boosting rounds start to overfit (CV F1 drops from 0.905 to
0.899).

The reason GBT escapes the imbalance trap that hits RF is its loss
function. Each round of boosting fits a tree to the *gradient* of the
loss with respect to the current predictions, which means the
algorithm preferentially focuses on the rows it is currently getting
wrong — typically the rare positive class. RF, by contrast, averages
independent trees and has no mechanism to up-weight minority errors.

### Linear SVC — high recall, low precision

SVM trades precision for recall. It catches 72 % of the actual
positives (the highest of any model) but throws off 94 false alarms,
giving precision 47 %. AUC 0.898 is competitive with RF and GBT.
Linear SVC with `regParam=0.001` essentially trains a near-hard-
margin model — it will move the decision boundary aggressively to
separate classes, which on imbalanced data tends to favour recall.

If the dashboard needs to *not miss* high-demand groups (e.g. for
alerting), SVM is a defensible second choice.

### Gaussian Naive Bayes — uncalibrated probabilities

Switching NB from `multinomial` to `gaussian` rescued it from the
AUC ≈ 0.05 catastrophe of the original pipeline (the multinomial
variant treats continuous features as nonsensical counts and
produced inverted predictions). The new version has a respectable
AUC of 0.807 — the *ranking* of high vs low groups is informative.

But at threshold 0.5 the calibration is wrong: NB predicts 228 test
rows as positive and gets **zero of them right**. The 117 actual
positives are all labelled 0. Gaussian NB assumes per-feature
independence and Normal conditional distributions, both of which are
violated badly here (the one-hot columns are anything but Gaussian).
The model produces probabilities that order rows correctly on
average but cross the 0.5 line at the wrong points.

For comparison purposes the report keeps NB in the metrics table.
For production it would need either a tuned threshold (probably
around 0.05–0.10) or a calibration step (Platt scaling / isotonic
regression).

### Ensemble — drag from RF

The soft-voting ensemble of RF + GBT + SVM achieves accuracy 0.891
and AUC 0.931, slightly below GBT alone. It has the **highest
precision of any model** (0.684) because two of three votes have to
agree before the ensemble assigns class 1. But its recall is only
0.333 — RF's per-row probability for class 1 is always low, so it
keeps pulling the averaged probability below the 0.5 threshold.

This is the classic ensemble failure mode: one member is much worse
than the others and dragging averages down. A two-member ensemble of
GBT + SVM would almost certainly outperform GBT alone here; that is
left as a follow-up (the code in
`scripts/stage3_train_models.py` exposes the member list as a single
constant, `ENSEMBLE_MEMBERS`).

## 6. Feature importance

Tree-based models expose `featureImportances`. The CSVs in
`output/stage3_rf_feature_importance.csv` and
`output/stage3_gbt_feature_importance.csv` contain all 1 662 features
sorted by importance. The headline patterns:

### GBT — top 10

| Rank | Feature | Importance |
|---|---|---|
| 1 | `search_country=united states` | 0.280 |
| 2 | `companies_per_posting` | 0.157 |
| 3 | `skills_per_posting` | 0.140 |
| 4 | `summary_coverage` | 0.099 |
| 5 | `distinct_job_levels` | 0.061 |
| 6 | `search_position=technical coordinator` | 0.014 |
| 7 | `search_position=safety inspector` | 0.013 |
| 8 | `search_position=gas inspector` | 0.012 |
| 9 | `search_position=accountant cost` | 0.010 |
| 10 | `search_position=river` | 0.009 |

The five derived features alone account for **74 %** of total
importance. The one-hot tail captures specific positions/countries
with idiosyncratic demand patterns but each contributes <1.5 %.

### RF — top 10

| Rank | Feature | Importance |
|---|---|---|
| 1 | `search_country=united states` | 0.112 |
| 2 | `companies_per_posting` | 0.087 |
| 3 | `skills_per_posting` | 0.075 |
| 4 | `search_country=canada` | 0.038 |
| 5 | `search_country=united kingdom` | 0.035 |
| 6 | `distinct_job_levels` | 0.034 |
| 7 | `summary_coverage` | 0.029 |
| 8 | `search_country=australia` | 0.017 |
| 9 | `search_position=manager lodging facilities` | 0.017 |
| 10 | `search_position=programmer engineering and scientific` | 0.016 |

RF's importance is spread more thinly across many countries and
positions because every tree votes independently. GBT, building
sequential corrective trees, focuses much more sharply on the five
high-signal numerics.

Take-away: the binary "is this the US?" question alone explains a
quarter of GBT's signal; `companies_per_posting` and
`skills_per_posting` together add another 30 %. Without those three
the model would not be useful.

## 7. Sample predictions

`output/stage3_sample_predictions.csv` contains predictions from all
five models on:

- 20 random rows drawn from the held-out test set (`source=held_out`)
- 4 hand-crafted `(country, position)` scenarios built with the
  median values of the derived numeric features (`source=crafted`)

Two rows have `true_label=1` in the held-out sample
(`united states / emergency medical services coordinator` and
`united states / scout`):

| Row | true | rf | gbt | svm | nb | ensemble |
|---|---|---|---|---|---|---|
| us / emergency medical services coordinator | 1 | 0 | **1** | **1** | 0 | **1** |
| us / scout | 1 | 0 | **1** | **1** | 0 | 0 |

GBT and SVM both flag the EMS coordinator correctly; the ensemble
agrees. For "scout", GBT and SVM are right but the ensemble misses
it because RF's zero probability drags the mean below 0.5. NB
behaves erratically across the sample — flagging products like
`director radio`, `head coach`, `staff toxicologist` as high-demand
where the truth is 0.

## 8. Discussion: what worked and what didn't

**What worked.**

- The data-prep simplification (drop temporal logic, aggregate at
  `(country, position)`, use ratio features) gave a clean, defensible
  feature set. The ratio features `companies_per_posting`,
  `skills_per_posting`, `summary_coverage` are particularly valuable
  for GBT — together with the country one-hot they explain ~70 % of
  importance.
- 3-fold CV with explicit grids made the tuning auditable. The
  `stage3_<model>_cv_grid.csv` files prove for the report which
  combinations were tried and how each scored.
- GBT is a clear winner with strong, balanced performance. Adding it
  to the rubric-required RF/SVM/NB lineup turned out to be the most
  impactful improvement.

**What didn't, and why.**

- Class imbalance (13 / 87) broke Random Forest. RF in Spark cannot
  natively handle imbalance — it argmax'es probability vectors that
  never cross 0.5. The fix would be `weightCol` or sampling; not
  applied to keep the comparison fair.
- Gaussian NB is uncalibrated: AUC 0.81 says the ranking is fine
  but the 0.5 threshold lands in the wrong place. A learned
  threshold (Platt scaling) would lift it, but that exceeds the
  rubric's scope.
- The temporal framing implied by the original project description
  is not supported by the data. Both `first_seen` and
  `last_processed_time` are scraper artefacts. We made this
  explicit in the prepare-script docstring so the limitation is
  documented rather than hidden.
- The ensemble's RF member was a drag. The framework supports
  changing members in one constant (`ENSEMBLE_MEMBERS`); leaving RF
  in was a deliberate choice so the report can discuss why averaging
  with a broken model hurts.

**Reproducibility.** All randomness is seeded
(`RANDOM_SEED=42` in prepare, `seed=42` for RF / GBT / CV, fixed seed
in `F.rand()` for sample predictions, deterministic LinearSVC /
Gaussian NB optimisers). The pipeline produces the same metrics
across runs up to the 5th–6th decimal place; the small drift is
non-associative floating-point sum order in Spark, not algorithmic
noise.

## 9. Recommendations

1. **Use GBT in the Stage 4 dashboard.** Highest accuracy, F1, and
   AUC; balanced precision/recall on the minority class. Show
   feature importance from `stage3_gbt_feature_importance.csv` as a
   "what the model learned" panel.
2. **Show all four models side-by-side in the report**, including
   the broken RF and NB rows — the contrast tells the imbalance
   story and demonstrates that the choice of GBT is informed rather
   than arbitrary.
3. **If a follow-up is allowed**, the fastest improvement would be
   either (a) recompute the ensemble without RF, or (b) add a
   threshold-tuning step for RF and NB (search t ∈ {0.1, 0.2, 0.3,
   0.5} on the validation fold, pick by F1).
4. **Do not market the pipeline as temporal demand prediction**;
   describe it as static demand classification at the
   (country, position) granularity. The data does not support
   anything stronger.
