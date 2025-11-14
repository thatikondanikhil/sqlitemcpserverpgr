import os
import json
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import argparse
from mcp.server.fastmcp import FastMCP
from mcp.server.stdio import stdio_server
import sys
import logging
logger = logging.getLogger(__name__)

load_dotenv()

mcp = FastMCP("SQLiteServer")
parser = argparse.ArgumentParser(description="FastMCP Server with dynamic DB path.")
parser.add_argument(
    "--db-path", 
    type=str, 
    required=True, 
    help="The file path for the SQLite database."
)

try:
    args, unknown = parser.parse_known_args()
except SystemExit:
    sys.exit(1)


DB_PATH = args.db_path
print(f"FastMCP Server initialized with DB Path: {DB_PATH}", file=sys.stderr)

class SQLiteHandler:
    """Handles basic SQLite operations such as queries and updates."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def execute_query(self, sql: str, values=None):
        """Executes a SELECT query and returns the rows as dictionaries."""
        try:
            if values is None:
                values = []
            cur = self.conn.execute(sql, values)
            rows = cur.fetchall()
            return True, [dict(row) for row in rows]
        except Exception as e:
            logger.exception(f"Query execution failed: {e}")
            return False, []

    def execute_run(self, sql: str, values=None):
        """Executes a data modification query (INSERT, UPDATE, DELETE)."""
        try:
            if values is None:
                values = []
            cur = self.conn.execute(sql, values)
            self.conn.commit()
            return True, {"lastID": cur.lastrowid, "changes": self.conn.total_changes}
        except Exception as e:
            logger.exception(f"Execution failed: {e}")
            return False, {}

    def list_tables(self):
        """Lists all user-created tables in the SQLite database."""
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        return self.execute_query(sql)

    def get_table_schema(self, table_name: str):
        """Retrieves schema information for a specific table."""
        sql = f"PRAGMA table_info({table_name})"
        return self.execute_query(sql)


@mcp.tool()
def db_info():
    """
    Retrieve metadata about the SQLite database.

    Returns information such as:
    - Database path
    - File existence
    - File size
    - Last modification time
    - Number of tables
    """
    try:
        db_path = DB_PATH
        abs_path = os.path.abspath(db_path)
        handler = SQLiteHandler(abs_path)

        exists = os.path.exists(abs_path)
        size = os.path.getsize(abs_path) if exists else 0
        modified = (
            datetime.fromtimestamp(os.path.getmtime(abs_path)).isoformat() if exists else None
        )
        status, tables = handler.list_tables()

        data = {
            "dbPath": abs_path,
            "exists": exists,
            "size": size,
            "lastModified": modified,
            "tableCount": len(tables) if status else 0,
        }

        logger.info("Database info fetched successfully.")
        return {"content": [{"type": "text", "text": json.dumps(data, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error fetching db_info: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def query(sql: str, values=None):
    """
    Execute a raw SQL SELECT query.

    Args:
        sql (str): SQL SELECT query.
        values (list, optional): Parameter values for the query.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        status, results = handler.execute_query(sql, values or [])
        if not status:
            return {"content": [{"type": "text", "text": "Query execution failed."}], "isError": True}

        logger.info("Query executed successfully.")
        return {"content": [{"type": "text", "text": json.dumps(results, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error executing query: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def listing_tables():
    """
    List all user tables in the SQLite database.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        status, tables = handler.list_tables()
        if not status:
            return {"content": [{"type": "text", "text": "Failed to list tables."}], "isError": True}
        logger.info("Listed all tables successfully.")
        return {"content": [{"type": "text", "text": json.dumps(tables, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error listing tables: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def get_table_schema(tableName: str):
    """
    Get the schema of a given table.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        status, schema = handler.get_table_schema(tableName)
        if not status:
            return {"content": [{"type": "text", "text": "Failed to get table schema."}], "isError": True}

        logger.info(f"Fetched schema for table '{tableName}'.")
        return {"content": [{"type": "text", "text": json.dumps(schema, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error fetching table schema: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def create_record(table: str, data: dict):
    """
    Insert a new record into a specified table.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        status, result = handler.execute_run(sql, list(data.values()))
        if not status:
            return {"content": [{"type": "text", "text": "Insert operation failed."}], "isError": True}

        logger.info(f"Record inserted successfully into table '{table}'.")
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error inserting record: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def read_records(table: str, conditions=None, limit=None, offset=None):
    """
    Read records from a specified table with optional filters.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        sql = f"SELECT * FROM {table}"
        values = []

        if conditions:
            where = " AND ".join(f"{k}=?" for k in conditions.keys())
            sql += f" WHERE {where}"
            values.extend(conditions.values())
        if limit is not None:
            sql += f" LIMIT {limit}"
            if offset is not None:
                sql += f" OFFSET {offset}"

        status, results = handler.execute_query(sql, values)
        if not status:
            return {"content": [{"type": "text", "text": "Failed to read records."}], "isError": True}

        logger.info(f"Records read successfully from table '{table}'.")
        return {"content": [{"type": "text", "text": json.dumps(results, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error reading records: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def update_records(table: str, data: dict, conditions: dict):
    """
    Update existing records in a table.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        set_clause = ", ".join(f"{k}=?" for k in data.keys())
        where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        values = list(data.values()) + list(conditions.values())
        status, result = handler.execute_run(sql, values)
        if not status:
            return {"content": [{"type": "text", "text": "Update operation failed."}], "isError": True}

        logger.info(f"Records updated successfully in table '{table}'.")
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error updating records: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


@mcp.tool()
def delete_records(table: str, conditions: dict):
    """
    Delete records from a specified table.
    """
    try:
        db_path = DB_PATH
        handler = SQLiteHandler(db_path)
        where_clause = " AND ".join(f"{k}=?" for k in conditions.keys())
        sql = f"DELETE FROM {table} WHERE {where_clause}"

        status, result = handler.execute_run(sql, list(conditions.values()))
        if not status:
            return {"content": [{"type": "text", "text": "Delete operation failed."}], "isError": True}

        logger.info(f"Records deleted successfully from table '{table}'.")
        return {"content": [{"type": "text", "text": json.dumps(result, indent=2)}]}
    except Exception as e:
        logger.exception(f"Error deleting records: {e}")
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}


if __name__ == "__main__":
    mcp.run(transport="stdio")


 