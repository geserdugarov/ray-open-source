# RLlib

**Location:** `rllib/`
**Purpose:** Scalable reinforcement learning library.

## Overview

RLlib is Ray's library for reinforcement learning. It provides production-grade implementations of popular RL algorithms, distributed training at scale, and support for multi-agent environments. RLlib is transitioning to a new "RLModule" API stack for improved modularity.

## Architecture

```
+-------------------------------------------------------------------+
|                        Algorithm                                  |
|  (PPO, DQN, A3C, SAC, IMPALA, etc.)                              |
|                                                                   |
|  +--------------------+    +--------------------+                 |
|  |   EnvRunner(s)     |    |   LearnerGroup     |                 |
|  |  (data collection) |    |  (gradient compute) |                 |
|  |                    |    |                    |                 |
|  |  Environment  <-+  |    |  +-> RLModule      |                 |
|  |  RLModule      |  |    |  |   (neural net)  |                 |
|  |  Connector ----+  |    |  |   Loss function  |                 |
|  +--------------------+    |  +-> Optimizer      |                 |
|                            +--------------------+                 |
+-------------------------------------------------------------------+
```

## Key Abstractions

### Algorithm (`algorithms/algorithm.py`, ~208 KB)
Base class for all RL algorithms. Manages the training loop:
1. Collect experience via EnvRunners
2. Compute gradients via LearnerGroup
3. Update model weights
4. Log metrics

Configuration via `AlgorithmConfig` (`algorithm_config.py`, ~317 KB) — the most detailed configuration object in Ray, covering environment, model, training, resources, and multi-agent settings.

### RLModule (`core/rl_module/`)
New modular neural network abstraction (replacing legacy `Policy`):
- Defines forward pass for inference and training
- Separable into encoder, head, and value components
- Supports PyTorch and TensorFlow backends
- `MultiRLModule` for multi-agent setups

### Policy (`policy/`)
Legacy abstraction combining network + learning logic:
- `policy.py` — Base Policy class
- `torch_policy_v2.py` — PyTorch policy
- `tf_policy_v2.py` — TensorFlow policy
- Being replaced by RLModule + Learner

### EnvRunner (`env/`)
Data collection workers that run environment rollouts:
- Step through environments using the current policy
- Collect trajectories (SampleBatch)
- Apply connector pipelines for observation/action processing

### Learner / LearnerGroup (`core/learner/`)
Distributed gradient computation:
- `Learner` — Single learner computing loss and gradients
- `LearnerGroup` — Manages multiple Learners across GPUs/nodes

### SampleBatch (`policy/sample_batch.py`)
Standard data format for RL experience:
- Contains observations, actions, rewards, dones, and metadata
- Used to pass data between EnvRunners and Learners

### Connectors (`connectors/`)
Modular pipeline for data processing between environment and model:
- Observation preprocessing
- Action postprocessing
- Reward shaping
- State management

## Built-in Algorithms

RLlib includes implementations for major RL algorithm families:

| Category | Algorithms |
|----------|-----------|
| Policy Gradient | PPO, A3C/A2C, IMPALA, APPO |
| Q-Learning | DQN, Rainbow, R2D2 |
| Actor-Critic | SAC, DDPG, TD3 |
| Model-Based | Dreamer, MBMPO |
| Multi-Agent | Independent, centralized critic, QMIX |
| Offline | CQL, MARWIL, BC |

## Key Directories

| Directory | Purpose |
|-----------|---------|
| `algorithms/` | Algorithm implementations and configs |
| `core/rl_module/` | New RLModule API (neural network modules) |
| `core/learner/` | Learner abstraction (gradient computation) |
| `env/` | Environment wrappers, EnvRunner, BaseEnv, MultiAgentEnv |
| `policy/` | Legacy Policy classes, SampleBatch |
| `models/` | Neural network model components |
| `connectors/` | Data processing pipelines |
| `evaluation/` | RolloutWorker (legacy data collection) |
| `execution/` | Distributed training orchestration |
| `offline/` | Offline/batch reinforcement learning |
| `examples/` | Example scripts and configurations |

## Relationships

- RLlib algorithms are **Ray Tune** trainables — HPO via `Tuner(algo, ...)`
- Uses **Ray Core** actors for distributed EnvRunners and Learners
- Can use **Ray Data** for offline RL dataset loading
- Trained policies can be deployed via **Ray Serve**
