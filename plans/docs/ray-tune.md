# Ray Tune

**Location:** `python/ray/tune/`
**Purpose:** Hyperparameter tuning and experiment management.

## Overview

Ray Tune is a scalable hyperparameter optimization framework. It runs multiple **trials** in parallel, each with a different hyperparameter configuration. A **search algorithm** proposes configurations and a **scheduler** decides which trials to continue, pause, or stop early.

## Key Abstractions

### Tuner (`tuner.py`)
The recommended modern entry point. Wraps a trainable with search space, search algorithm, scheduler, and configuration:

```python
tuner = Tuner(
    trainable,
    param_space={"lr": tune.loguniform(1e-4, 1e-1)},
    tune_config=TuneConfig(num_samples=100, scheduler=ASHAScheduler()),
    run_config=RunConfig(name="experiment"),
)
results = tuner.fit()
```

### Trainable (`trainable/`)
A unit of computation that Tune can optimize:
- **FunctionTrainable** — a Python function that reports metrics via `ray.train.report()`
- **Class Trainable** — a class with `setup()`, `step()`, `save_checkpoint()`, `load_checkpoint()` methods
- Ray Train trainers are also valid trainables

### Trial (`experiment/`)
A single training run with a specific hyperparameter configuration. Has states: PENDING, RUNNING, PAUSED, TERMINATED, ERROR.

### Search Algorithms (`search/`)
Propose hyperparameter configurations:
- `BasicVariantGenerator` — Grid search and random search (built-in)
- Integration-based: Bayesian optimization (Optuna, BayesOpt), evolutionary (HyperOpt, Nevergrad), etc.
- `SearchAlgorithm` — Base class for custom searchers

### Trial Schedulers (`schedulers/`)
Control trial execution — which to continue, pause, or stop:
- `FIFOScheduler` — First in, first out (default)
- `ASHAScheduler` / `HyperBandScheduler` — Aggressive early stopping of bad trials
- `PopulationBasedTraining` (PBT) — Mutate hyperparameters of good trials
- `MedianStoppingRule` — Stop trials below median performance

### ResultGrid (`result_grid.py`)
Analysis interface for completed tuning runs. Access best trial, all trial results, and experiment metrics.

### Callbacks (`callback.py`)
Lifecycle hooks for custom logic:
- `on_trial_start`, `on_trial_result`, `on_trial_complete`, `on_trial_error`
- `on_experiment_end`

### TuneController (`execution/`)
Internal orchestrator that manages trial scheduling, resource allocation, and state tracking.

## Key Files

| File | Purpose |
|------|---------|
| `tuner.py` | Tuner entry point |
| `tune.py` | `tune.run()` / `tune.run_experiments()` (45 KB) |
| `trainable/` | Trainable base class, FunctionTrainable |
| `experiment/` | Experiment, Trial definitions |
| `search/` | Search algorithms |
| `schedulers/` | Trial schedulers (ASHA, PBT, FIFO) |
| `result_grid.py` | Result analysis |
| `callback.py` | Lifecycle callbacks |
| `execution/` | TuneController orchestration |
| `progress_reporter.py` | Console/notebook progress reporting (57 KB) |
| `analysis/` | ExperimentAnalysis utilities |
| `integration/` | Framework integrations (Lightning, etc.) |

## Relationships

- Tunes **Ray Train** trainers (the most common use case)
- Tunes **RLlib** algorithms (RLlib algorithms are Tune trainables)
- Configured via **Ray AIR** (`RunConfig`, `ScalingConfig`)
- Built on **Ray Core** actors for parallel trial execution
