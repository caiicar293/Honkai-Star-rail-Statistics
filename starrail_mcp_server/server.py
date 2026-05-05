"""
MCP Server for Honkai Star Rail DuckDB Database
Connects Claude to: D:\OneDrive\Honkai_star_rail_Scanner\starrail_data.db

Requirements:
    pip install mcp duckdb
"""

import json
import duckdb
from pathlib import Path
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types
import os 
from dotenv import load_dotenv
load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────────
DB_PATH = f'../{os.getenv("DB_File")}'

# ── Server setup ─────────────────────────────────────────────────────────────
app = Server("starrail-duckdb")

# ── Helpers ──────────────────────────────────────────────────────────────────
def run_query(sql: str) -> list[dict]:
    with duckdb.connect(DB_PATH, read_only=True) as con:
        rel = con.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]

# ── Tools ────────────────────────────────────────────────────────────────────
@app.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_tables",
            description="List all tables in the Star Rail DuckDB database.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        types.Tool(
            name="describe_table",
            description="Show the columns and data types for a specific table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name to describe"}
                },
                "required": ["table"],
            },
        ),
        types.Tool(
            name="query",
            description=(
                "Run a read-only SQL SELECT query against the Star Rail DuckDB database. "
                "Returns up to 500 rows. Only SELECT statements are allowed."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL SELECT statement to execute"}
                },
                "required": ["sql"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    if not Path(DB_PATH).exists():
        return [types.TextContent(type="text", text=f"Error: Database not found at {DB_PATH}")]

    try:
        if name == "list_tables":
            rows = run_query("SHOW TABLES")
            tables = [r["name"] for r in rows]
            text = "Tables in database:\n" + "\n".join(f"  • {t}" for t in tables)

        elif name == "describe_table":
            table = arguments["table"]
            cols = run_query(f"DESCRIBE {table}")
            lines = [f"  {c['column_name']} ({c['column_type']})" for c in cols]
            text = f"Columns in '{table}':\n" + "\n".join(lines)

        elif name == "query":
            sql = arguments["sql"].strip()
            forbidden = ("insert", "update", "delete", "drop", "create", "alter", "truncate")
            if any(sql.lower().startswith(kw) for kw in forbidden):
                return [types.TextContent(type="text", text="Error: Only SELECT queries are allowed.")]
            rows = run_query(sql)[:500]
            if not rows:
                text = "Query returned no results."
            else:
                text = f"{len(rows)} row(s) returned:\n" + json.dumps(rows, indent=2, default=str)

        else:
            text = f"Error: Unknown tool '{name}'"

    except Exception as exc:
        text = f"Error: {exc}"

    return [types.TextContent(type="text", text=text)]


# ── Entry point ──────────────────────────────────────────────────────────────
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())