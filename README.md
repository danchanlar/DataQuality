# DQ Tool

Standalone Python DQ project with external SQL path discovery from CSB2DATA.

## Overview

DQ Tool is a centralized, independent Python-based data quality management system that:
- Reads SQL files from an external path reference (CSB2DATA) in read-only mode
- Discovers and infers data quality rules from code relationships
- Prevents duplicate rule imports via deduplication
- Executes rules against a target database with configurable parallelism
- Tracks execution sessions, violations, and rule history

## Migration Status (from CSB2DATA)

### What "basic migration" means

In this project, "basic migration" means the core app structure has already been moved and adapted so the DQ Tool runs from its own folder and environment.

Completed work:
- Copied full Streamlit UI layer from CSB2DATA into DQ_Tool:
   - `pages` (1-7)
   - `components`
   - `utils`
- Copied `dq_engine` module into DQ_Tool for execution, SQL generation, DB pool, and persistence features.
- Updated imports to the new package layout:
   - `components.*` -> `dq_tool.ui.components.*`
   - `utils.*` -> `dq_tool.ui.utils.*`
- Updated module path bootstrapping (`ROOT` / `sys.path`) so imports resolve correctly under DQ_Tool.
- Added/kept Configuration support for External SQL Root Path and connected Auto Discovery to use this configured path by default.
- Verified app startup on a free port after migration.

### What this does NOT imply

"Basic migration" does not mean every module has been deeply refactored for a brand new architecture yet.
It means the existing CSB2DATA functionality has been transferred and wired so it can run from DQ_Tool as an independent app.

## Setup & Installation

### Prerequisites
- Python 3.10+
- Windows PowerShell or compatible shell
- Network access to CSB2DATA folder and SQL Server database

### Quick Start

1. **Create & Activate Virtual Environment**
   ```powershell
   cd c:\Users\d.chalandrinou\Desktop\DQ_Tool
   .\.venv\Scripts\Activate.ps1
   ```

2. **Install Dependencies** (already done)
   ```powershell
   .\.venv\Scripts\python -m pip install --upgrade pip
   .\.venv\Scripts\python -m pip install streamlit pyodbc pytest networkx sqlparse pyvis
   ```

3. **Run Application**
   ```powershell
   .\run.ps1
   ```
   Or manually:
   ```powershell
   .\.venv\Scripts\python -m streamlit run dq_tool/ui/app.py --server.port 8520
   ```

   The app will be available at: **http://localhost:8520**

## Project Structure

```
DQ_Tool/
├── dq_tool/                           # Main package
│   ├── ui/                            # Streamlit UI
│   │   ├── app.py                     # Main Streamlit app
│   │   ├── pages/                     # Multi-page UI (1-7)
│   │   │   ├── 1_Rule_Definition.py
│   │   │   ├── 2_Rule_Management.py
│   │   │   ├── 3_Execution_Dashboard.py
│   │   │   ├── 4_History_Audit.py
│   │   │   ├── 5_Auto_Discovery.py
│   │   │   ├── 6_Configuration.py
│   │   │   └── 7_Execution_Logs.py
│   │   ├── components/                # Reusable UI components
│   │   └── utils/                     # Pagination, logging, state
│   ├── adapters/                      # SQL source adapters
│   │   ├── sql_source_adapter.py      # Protocol definition
│   │   └── external_path_source.py    # External path reader
│   ├── discovery/                     # Rule discovery pipeline
│   │   ├── scanner.py
│   │   ├── sql_parser.py
│   │   ├── relationship_inference.py
│   │   ├── candidate_builder.py
│   │   └── deduplicator.py
│   ├── engine/                        # Rule execution
│   │   ├── rule_executor.py
│   │   ├── rule_types.py
│   │   └── session_runner.py
│   ├── persistence/                   # Database repositories
│   │   ├── db_pool.py
│   │   ├── rules_repo.py
│   │   ├── sessions_repo.py
│   │   └── violations_repo.py
│   ├── api/                           # DTOs & services
│   ├── logging/                       # Logging configuration
│   └── reports/                       # Report generation
├── tests/                             # Unit tests
├── pyproject.toml                     # Project metadata & dependencies
├── .env.example                       # Environment variables template
├── .gitignore
├── README.md
└── run.ps1                            # Streamlit launcher script

```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and customize:

```env
DQ_SQL_SOURCE_MODE=external_path
DQ_SQL_ROOT_PATH=C:/Users/d.chalandrinou/Desktop/CSB2DATA
DQ_SQL_INCLUDE_GLOBS=**/*.sql
DQ_SQL_EXCLUDE_GLOBS=**/bin/**;**/obj/**;**/.git/**;**/__pycache__/**
DQ_DB_SERVER=SANDBOX-SQL\\MSSQL2022
DQ_DB_DATABASE=CSBDATA_DEV
```

## Key Features

### Auto Discovery
- Scans SQL files from external CSB2DATA path
- Parses joins, foreign keys, and relationships
- Generates rule candidates with confidence scoring
- Deduplicates against existing rules in DQ.Rules table
- Dynamically loads DataQuality modules (`relationship_graph.py`, `discovery_report.py`)

### Rule Management
- Create rules with 19 configurable rule types
- Bulk edit & manage rules with pagination
- Track rule lineage and discovery evidence

### Execution Dashboard
- Execute filtered rule sets
- Live progress tracking with auto-refresh (2s interval)
- Parallel rule execution (configurable max_workers)
- Connection pool sizing: `pool_size = max_workers + 2`

### History & Audit
- Track all execution sessions
- Inspect violations per rule
- Audit trail with timestamps

## Development

### Run Tests
```powershell
.\.venv\Scripts\pytest tests/
```

### Install New Dependencies
```powershell
.\.venv\Scripts\python -m pip install <package_name>
```

## Database Schema

DQ Tool uses the `DQ` schema in the target database:
- `DQ.Rules` - Rule definitions and metadata
- `DQ.RuleExecutions` - Execution history per rule
- `DQ.RuleViolations` - Violations detected per rule execution
- `DQ.ExecutionSessions` - Batch execution sessions

## Design Principles

1. **External Path Reference**: SQL files are read-only from CSB2DATA
2. **Separation of Concerns**: Python logic independent of SQL asset versioning
3. **Deduplication**: No duplicate rules imported from Auto Discovery
4. **Thread Safety**: Connection pooling with configurable parallelism
5. **Auditability**: All discovery runs logged with version/timestamp

## Troubleshooting

### Streamlit won't start
- Ensure `.venv` exists: `py -3 -m venv .venv`
- Verify dependencies: `.\.venv\Scripts\python -m pip list | findstr streamlit`

### Port is not available
- Run with another port: `.\run.ps1 8521`
- Or manually: `.\.venv\Scripts\python -m streamlit run dq_tool/ui/app.py --server.port 8521`

### Can't find CSB2DATA SQL files
- Check `DQ_SQL_ROOT_PATH` in `.env` points to correct location
- Verify permissions to read SQL files
- Ensure `include_globs` pattern matches your SQL file locations

### Auto Discovery import error (e.g. `No module named 'networkx' or `No module names pyvis')
- Install discovery dependencies in the DQ_Tool venv:
   - `.\.venv\Scripts\python -m pip install networkx sqlparse`
- These packages are required by the DataQuality parser/graph modules loaded by Auto Discovery.

### Verify Auto Discovery dependency chain
- Run:
   - `.\.venv\Scripts\python -c "import sys; sys.path.insert(0, r'c:\Users\d.chalandrinou\Desktop\DataQuality'); import relationship_graph; print('REL_GRAPH_IMPORT_OK')"`
- If `REL_GRAPH_IMPORT_OK` is printed, the discovery module imports are healthy.

### Database connection fails
- Verify `DQ_DB_SERVER` and `DQ_DB_DATABASE` in Configuration page
- Confirm SQL Server is accessible from this machine
- Check Windows Authentication or SQL credentials

