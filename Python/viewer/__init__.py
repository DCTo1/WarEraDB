"""Local read-only web viewer for the WarEra DB (package).

Entry point: Python/db_web.py (thin CLI: args, scheduler, HTTP server).
Modules:
    config     constants + runtime settings (db name, ranking latest)
    queries    read-only query helpers (SQLAlchemy via Python/db.py)
    search     prefix search module (users active/inactive + countries; /search)
    updater    background auto-updater thread (state, scheduler, /update-status)
    ui         layout, escaping, theme/JS assets
    pages/     one module per page (all SQL + HTML lives there)
    server     HTTP handler + route table
"""
