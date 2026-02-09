

# Honkai Star Rail Statistical Analysis Suite

A professional-grade Python suite for the statistical analysis of Honkai: Star Rail combat data. This engine provides deep insights into character performance, team synergies, and archetype efficiency using large-scale datasets.

## 🚀 Key Features

* **Multi-Mode Support:** Dedicated analysis engines for Memory of Chaos (MoC), Pure Fiction, and Apocalyptic Shadow and Anomaly Arbitration.
* **Graph Theory Integration:** Uses `NetworkX` and **Eigenvector Centrality** to visualize and rank character influence within team compositions.
* **Data Mining:** Implements the **Apriori Algorithm** to discover hidden character synergies and "market basket" team associations.
* **Performance Benchmarking:** Calculates percentiles (, , ) for cycles and scores to provide a realistic view of character power.
* **Dynamic Data Fetching:** Automatically pulls the latest datasets from Hugging Face and character metadata from GitHub.

## 📁 Project Structure

| File | Game Mode | Primary Metric |
| --- | --- | --- |
| `Appearance_rate.py` | **Memory of Chaos** | Cycles used to clear. |
| `Appearance_rate_Pure_fiction.py` | **Pure Fiction** | Total points scored. |
| `Appearance_rate_Apocalytic_Shadow.py` | **Apocalyptic Shadow** | Action Value / Scores. |
| `Appearance_rate_anomaly.py` | **Anomaly Arbitration** | Cycles used to clear. |

## 🛠️ Requirements

```bash
pip install pandas matplotlib numpy networkx requests

```

## 🔬 Core Analytics Features

### **1. Multi-Dimensional Statistics**

Unlike basic trackers, this engine provides a full statistical profile for every team and character:

* **Central Tendency:** Mean, Median, and Mode of cycle counts.
* **Volatility Tracking:** Standard Deviation and Min/Max cycle spreads.
* **Performance Tiers:** Automatic calculation of 25th and 75th percentile performance.

### **2. Dual-Node (Full Run) Analysis**

Analyze the relationship between the two teams used in a single clear.

* **Correlation:** Track how specific Side 1 teams impact the available options and performance of Side 2.
* **UID Continuity:** Maintains player identity across data nodes to ensure full-run integrity.

### **3. Meta-Archetyping**

The engine abstracts character names into roles (e.g., "Damage Dealer").

* Identify whether "Hypercarry," "Dual DPS," or "No-Sustain" archetypes are trending in the current game version.

### **4. Granular Filtering**

* **Eidolon Isolation:** Filter statistics by specific character resonance levels (e.g., analysis of E0-only teams).
* **Sustain Conditions:** Isolate "No-Sustain" runs to identify high-skill ceiling compositions.
* **Exempt Logic:** Handles complex character variants (e.g., different forms of the Trailblazer or Limited characters).

## 🛠️ Implementation Details

The project utilizes an **Object-Oriented** architecture centered around the `HonkaiStatistics` class.

| Module | Purpose |
| --- | --- |
| `_process_data` | Internal engine that cleans raw CSV data, filters by version/floor, and handles Eidolon logic. |
| `print_appearance_rates` | Generates a ranked leaderboard of the most popular and efficient team compositions. |
| `print_archetypes` | Summarizes performance by team structure rather than individual units. |
| `network_graph` | Visualizes the meta as a synergistic web using Eigenvector Centrality. |

## 🚀 Quick Start

```python
from Appearance_rate import HonkaiStatistics

# Analyze Memory of Chaos, Floor 12, Version 2.4
# Restrict to E0-E2 characters clearing within 10 cycles
moc_stats = HonkaiStatistics(version="2.4", floor=12, by_ed=2, by_cycle=10)

# 1. Print Character Appearance Rates (for Damage Dealers)
moc_stats.print_appearance_rate_by_char(damage_dealers_only=True)

# 2. Analyze Full Run (Both Sides)
moc_stats.print_appearance_rates_both_sides(least=50)

```

## 📊 Data Sourcing

The suite is designed for automation, pulling live data from:

* **Hugging Face:** For large-scale community clear datasets.
* **GitHub:** For real-time character metadata and role mappings.

