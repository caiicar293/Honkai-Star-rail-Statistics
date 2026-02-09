

# Honkai: Star Rail Statistical Analysis Suite

An advanced data science and graph theory toolkit designed to analyze the "Meta" of *Honkai: Star Rail*. This suite processes large-scale player data to determine character appearance rates, team synergies, and performance benchmarks across all major end-game modes.

## 🚀 Key Features

* **Multi-Mode Support:** Dedicated analysis engines for Memory of Chaos (MoC), Pure Fiction, and Apocalyptic Shadow.
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

## 📊 Sample Usage

The project is built using an Object-Oriented approach. You can initialize the statistics for a specific game version and floor:

```python
from Appearance_rate import HonkaiStatistics

# Initialize MoC Version 2.3.3, Floor 12
stats = HonkaiStatistics(version="2.3.3", floor=12)

# Generate a Synergy Network Graph
stats.network_graph(graph=True)

# Print Team Appearance Rates
stats.print_appearance_rates()

```

## 🧬 Technical Implementation Note

This suite goes beyond basic counting. It treats the game meta as a **Mathematical Graph** where:

* **Nodes** = Characters
* **Edges** = Frequency of appearing together
* **Weight** = Success rate/Performance of that specific pairing

By calculating **Centrality**, the code identifies "Core" characters that define the current meta versus "Niche" characters that only appear in specific teams.


