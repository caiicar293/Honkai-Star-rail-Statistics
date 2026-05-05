# Honkai Star Rail Statistics - Data Warehouse & Analytics Platform

A comprehensive data infrastructure for Honkai: Star Rail endgame statistics, combining ETL pipelines, DuckDB data warehousing, statistical analysis engines, and an MCP server for AI agent integration.

## 📊 What This Repository Contains

This project transforms raw community-sourced Honkai: Star Rail combat data into a queryable analytics platform with three main components:

1. **DuckDB Data Warehouse** - 40+ pre-computed statistical tables covering all endgame modes
2. **Statistical Analysis Scripts** - Python-based analytics engines (v1: Pandas, v2: Polars) 
3. **MCP Server** (Optional) - Model Context Protocol server for AI agent integration

## 🗄️ Data Warehouse

### Database Files
The repository includes multiple DuckDB database versions:
- `honkai_star_rail_stats_polars_v7.duckdb` - Latest production database
- Previous versions (v1-v6) retained for schema evolution tracking

All databases are also available on **[Hugging Face: caiicar293/Honkai-starrail-statistics](https://huggingface.co/datasets/caiicar293/Honkai-starrail-statistics)**

### Schema Overview (40+ Tables)

**Per Game Mode** (Memory of Chaos, Pure Fiction, Apocalyptic Shadow, Anomaly):
- `{mode}_stats_teams` - Team composition performance metrics
- `{mode}_stats_duos` - Character pair synergy analysis  
- `{mode}_stats_archetypes` - Role-based team composition patterns
- `{mode}_stats_distributions` - Performance distribution curves
- `{mode}_stats_distributions_summaries` - Statistical summaries (mean, median, percentiles)
- `{mode}_stats_dual_teams` - Cross-side team correlation (both sides)
- `{mode}_stats_dual_distributions` - Full-run performance distributions
- `{mode}_stats_gear_usage` - Light cone and relic set tracking

**Cross-Mode Meta Analytics:**
- `team_meta_summary` - Aggregated team performance across versions
- `archetype_meta_summary` - Meta trends by playstyle
- `duos_meta_summary` - Character pair synergy rankings
- `character_stats` - Individual character metadata and roles

### Data Source Attribution
- **Raw Data Source**: [LvlUrArti/MocData](https://huggingface.co/datasets/LvlUrArti/MocData) on Hugging Face
  - Community-contributed combat data from Honkai: Star Rail players
  - Covers Memory of Chaos, Pure Fiction, Apocalyptic Shadow, and Anomaly Arbitration
  
- **Processed Database**: [caiicar293/Honkai-starrail-statistics](https://huggingface.co/datasets/caiicar293/Honkai-starrail-statistics)
  - Pre-computed aggregations and statistical summaries
  - Optimized for analytical queries via DuckDB

## 📁 Repository Structure

### Analysis Scripts (V1 - Pandas-based)
Original Python implementations using pandas and nested loops:
- `Appearance_rate.py` - Memory of Chaos analysis
- `Appearance_rate_Pure_fiction.py` - Pure Fiction analysis
- `Appearance_rate_Apocalytic_Shadow.py` - Apocalyptic Shadow analysis
- `Appearance_rate_anomaly.py` - Anomaly Arbitration analysis

### Analysis Scripts (V2 - Polars-based)
High-performance rewrites using Polars DataFrames:
- `Appearance_rate_V2.py` - Memory of Chaos (Polars)
- `Appearance_rate_Pure_fiction_V2.py` - Pure Fiction (Polars)
- `Appearance_rate_Apocalytic_Shadow_V2.py` - Apocalyptic Shadow (Polars)
- `Appearance_rate_anomaly_V2.py` - Anomaly Arbitration (Polars)

### Database Pipeline
ETL scripts that build and maintain the DuckDB warehouse:
- `database_main.py` - **Master ETL orchestrator** (main entry point)
- `database_teams_summary.py` - Team aggregation logic
- `database_duos_summary.py` - Pair synergy calculations
- `database_distributions_summary.py` - Statistical distribution analysis

### Infrastructure
- `starrail_mcp_server/` - Model Context Protocol server implementation
- `characters.json` - Character metadata and role mappings

## 🚀 Usage

### Option 1: Use the Pre-Built Database

Download from Hugging Face and query directly:

```python
import duckdb
from huggingface_hub import hf_hub_download

# Download the database
db_path = hf_hub_download(
    repo_id="caiicar293/Honkai-starrail-statistics",
    filename="honkai_star_rail_stats_polars_v7.duckdb",
    repo_type="dataset"
)

# Connect and query
conn = duckdb.connect(db_path, read_only=True)

# Example: Top 10 teams in MoC 2.7.3 Floor 12
result = conn.execute("""
    SELECT team, avg_cycle, usage_count, median_cycle
    FROM moc_stats_teams
    WHERE version = '2.7.3' AND floor = 12
    ORDER BY usage_count DESC
    LIMIT 10
""").fetchdf()

print(result)
```

### Option 2: Run the Analysis Scripts

#### V1 Scripts (Pandas-based)
```python
from Appearance_rate import HonkaiStatistics

# Analyze Memory of Chaos Floor 12, Version 2.7.3
# Filter for E0-E2 characters, max 10 cycles
stats = HonkaiStatistics(version="2.7.3", floor=12, by_ed=2, by_cycle=10)

# Character appearance rates (damage dealers only)
stats.print_appearance_rate_by_char(damage_dealers_only=True)

# Team composition rankings (minimum 50 clears)
stats.print_appearance_rates_both_sides(least=50)

# Archetype meta breakdown
stats.print_archetypes()

# Network graph visualization
stats.network_graph(least=100)
```

#### V2 Scripts (Polars-based - Faster)
```python
from Appearance_rate_V2 import HonkaiStatistics_V2

# Same API, but uses Polars under the hood for better performance
stats = HonkaiStatistics_V2(version="2.7.3", floor=12)
stats.get_char_df()
```

### Option 3: Rebuild the Data Warehouse

```bash
# Install dependencies
pip install duckdb polars pandas numpy networkx matplotlib requests huggingface_hub

# Run the ETL pipeline
python database_main.py
```

### Option 4: MCP Server (AI Agent Integration)

The `starrail_mcp_server/` directory contains a Model Context Protocol server that allows AI agents (like Claude) to query the database directly.

```bash
# Configure in your MCP client
# Server provides read-only access to all DuckDB tables
```

## 🔬 Analytics Features

The analysis scripts (both V1 and V2) provide:

### 1. Multi-Dimensional Statistics
- **Central Tendency**: Mean, median, mode of cycle counts/scores
- **Volatility Tracking**: Standard deviation, min/max spreads
- **Performance Tiers**: 25th and 75th percentile benchmarking

### 2. Dual-Node Analysis
Tracks both sides of a full clear:
- **Cross-Side Correlation**: How Side 1 choices affect Side 2 performance
- **UID Continuity**: Maintains player identity across both nodes
- **Archetype Pairing**: Identifies winning archetype combinations

### 3. Meta-Archetyping
Abstracts teams into strategic patterns:
- Hypercarry (1 main DPS + 3 supports)
- Dual DPS (2 damage dealers)
- No-Sustain (high-risk offensive teams)
- DOT (damage over time focused)
- Break (weakness break exploitation)

### 4. Character Synergy Mining
- **Apriori Algorithm**: Discovers frequent character pairs
- **Duo Analysis**: Statistical strength of 2-character combinations
- **Network Graphs**: Eigenvector centrality rankings via NetworkX

### 5. Granular Filtering
- **Eidolon Tiers**: E0, E1-E2, E3-E5, E6 performance isolation
- **Version Tracking**: Cross-patch meta evolution
- **Floor-Specific**: Per-floor difficulty adjustment
- **Performance Cutoffs**: Filter by cycle/score thresholds

## 📈 Use Cases

### For Players
- Compare team compositions and find optimal builds
- See realistic performance expectations at your eidolon levels
- Discover underrated character synergies

### For Content Creators
- Generate data-driven tier lists and meta reports
- Track character/team popularity trends over time
- Create visualizations from pre-aggregated data

### For Researchers & Developers
- Query structured endgame data via SQL
- Build custom analytics on top of the warehouse
- Integrate with AI agents via MCP server

## 🛠️ Technical Stack

- **Database**: DuckDB (columnar OLAP database)
- **Data Processing**: Polars (V2), Pandas (V1)
- **Analysis**: NumPy, SciPy for statistical functions
- **Visualization**: Matplotlib, NetworkX for graphs
- **Data Source**: Hugging Face Datasets API
- **MCP Server**: Model Context Protocol for AI integration

## 📊 Database Query Examples

```sql
-- Top 10 most popular teams in current MoC
SELECT team, usage_count, avg_cycle, median_cycle
FROM moc_stats_teams
WHERE version = '2.7.3' AND floor = 12
ORDER BY usage_count DESC
LIMIT 10;

-- Character duo synergies
SELECT char1, char2, usage_count, avg_performance
FROM moc_stats_duos
WHERE version = '2.7.3'
ORDER BY usage_count DESC
LIMIT 20;

-- Archetype meta breakdown
SELECT archetype, COUNT(*) as team_count, AVG(avg_cycle) as avg_performance
FROM moc_stats_archetypes
WHERE version = '2.7.3' AND floor = 12
GROUP BY archetype
ORDER BY team_count DESC;

-- Performance distribution for a specific team
SELECT percentile_25, median_cycle, percentile_75, std_dev
FROM moc_stats_distributions_summaries
WHERE team = 'Acheron,Aventurine,Pela,Sparkle' 
  AND version = '2.7.3' 
  AND floor = 12;
```

## 🔄 Data Pipeline Architecture

```
Raw Data (Hugging Face: LvlUrArti/MocData)
    ↓
database_main.py (Master ETL Orchestrator)
    ├─→ Data cleaning & normalization
    ├─→ Team aggregation (database_teams_summary.py)
    ├─→ Duo synergy calculation (database_duos_summary.py)
    ├─→ Distribution analysis (database_distributions_summary.py)
    └─→ Archetype classification
    ↓
DuckDB Warehouse (honkai_star_rail_stats_polars_v7.duckdb)
    ↓
    ├─→ Analysis Scripts (Appearance_rate*.py)
    ├─→ MCP Server (AI agent queries)
    └─→ Hugging Face (caiicar293/Honkai-starrail-statistics)
```

## 📋 Schema Documentation

### Team Statistics Table
```sql
CREATE TABLE moc_stats_teams (
    version VARCHAR,
    floor INT,
    node VARCHAR,
    team VARCHAR,              -- Sorted character names
    avg_cycle DOUBLE,
    usage_count INT,
    median_cycle DOUBLE,
    percentile_25 DOUBLE,
    percentile_75 DOUBLE,
    std_dev DOUBLE,
    min_cycle INT,
    max_cycle INT
);
```

### Duo Synergy Table
```sql
CREATE TABLE moc_stats_duos (
    char1 VARCHAR,
    char2 VARCHAR,
    usage_count INT,
    avg_performance DOUBLE,
    version VARCHAR,
    floor INT
);
```

### Archetype Summary
```sql
CREATE TABLE moc_stats_archetypes (
    archetype VARCHAR,         -- e.g., "Hypercarry", "Dual DPS"
    avg_cycle DOUBLE,
    usage_count INT,
    team_examples TEXT,
    version VARCHAR,
    floor INT
);
```

## 🔗 Related Links

- **Processed Database**: [caiicar293/Honkai-starrail-statistics on Hugging Face](https://huggingface.co/datasets/caiicar293/Honkai-starrail-statistics)
- **Source Data**: [LvlUrArti/MocData on Hugging Face](https://huggingface.co/datasets/LvlUrArti/MocData)
- **DuckDB Documentation**: [duckdb.org](https://duckdb.org/)
- **Polars Documentation**: [pola.rs](https://pola.rs/)

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/Honkai-Star-rail-Statistics.git
cd Honkai-Star-rail-Statistics

# Install dependencies
pip install duckdb polars pandas numpy networkx matplotlib requests huggingface_hub

# Optional: Install MCP server dependencies
cd starrail_mcp_server
pip install -r requirements.txt
```

## 🤝 Contributing

Contributions welcome! Key areas:
- **Data Quality**: Improve cleaning and validation logic
- **New Metrics**: Add additional statistical measures
- **Visualization**: Create dashboards or charts
- **Documentation**: Expand examples and tutorials

## 📜 License & Attribution

### Data Attribution
- **Original Raw Data**: [LvlUrArti/MocData](https://huggingface.co/datasets/LvlUrArti/MocData) - Community-sourced Honkai: Star Rail combat clear data
- **Processed Database**: [caiicar293/Honkai-starrail-statistics](https://huggingface.co/datasets/caiicar293/Honkai-starrail-statistics) - Derived analytics and aggregations

All game content and character names are property of HoYoverse/Cognosphere.

## 🔮 Potential Extensions

- Real-time dashboard with Streamlit/Gradio
- REST API for external integrations
- Machine learning models for team performance prediction
- Cross-server (Asia/EU/NA) comparative analysis
- Historical meta trend visualization

---

*This is a community data project for analyzing Honkai: Star Rail endgame meta through statistical methods. All game data belongs to HoYoverse.*