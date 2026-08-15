"""Page handlers — one module per page.

Every page takes a parsed query-params dict (`q`) and returns an HTML string
(or an error page). SQL lives in the page modules alongside the markup, as in
the original extra/db_web.py.
"""

from .battle import page_battle
from .battles import page_battles
from .bounties import page_bounties
from .countries import page_countries

from .overview import page_overview
from .snipes import page_snipes
from .sql import page_sql
from .stats import page_stats
from .tracker import page_tracker
from .transactions import page_transactions
from .transactions_coverage import page_transactions_coverage
from .tx_priority import page_tx_priority
from .usage import page_usage
from .user import page_user
from .users import page_users
from .weekly import page_weekly

__all__ = [
    "page_overview", "page_battles", "page_battle", "page_users", "page_user",
    "page_bounties", "page_countries", "page_stats", "page_sql", "page_weekly",
    "page_tracker", "page_transactions",
    "page_transactions_coverage", "page_usage", "page_tx_priority",
    "page_snipes",
]
