# Star Rail DuckDB MCP Server — Setup Guide

## 1. Install dependencies
```bash
pip install duckdb
```

## 2. Copy server.py
Place `server.py` somewhere stable, e.g.:
```
D:\OneDrive\Honkai_star_rail_Scanner\starrail_mcp_server\server.py
```

## 3. Add to Claude Desktop config
Open your Claude Desktop config file at:
```
%APPDATA%\Claude\claude_desktop_config.json
```

Paste in the contents of `claude_desktop_config.json` (merge with any existing entries).
Update the path in `"args"` to wherever you saved `server.py`.

## 4. Restart Claude Desktop
Fully quit and relaunch Claude Desktop. The MCP server will appear in your tools.

---

## Available Tools (what Claude can do)
| Tool | What it does |
|---|---|
| `list_tables` | Shows all tables in the DB |
| `describe_table` | Shows columns + types for a table |
| `query` | Runs any SELECT query, returns up to 500 rows |

## Notes
- **Read-only** — INSERT, UPDATE, DELETE, DROP etc. are all blocked
- The DB path is hardcoded in `server.py` — update it if you move the file
- Requires Python 3.10+ and the `duckdb` pip package
