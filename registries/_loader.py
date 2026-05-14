"""SQL file loader for TABLE_REGISTRY entries.

Resolves paths relative to the project root (parent of this file's parent),
so it works regardless of the working directory when the flow is invoked.

Usage:
    from registries._loader import load_sql

    'p_query_join': load_sql('erp_join_ttIND_LOS.sql'),
"""

from __future__ import annotations

from pathlib import Path

_SQL_DIR = Path(__file__).parent.parent / 'sql'


def load_sql(filename: str) -> str:
    """Return the content of sql/<filename>, stripped of leading/trailing whitespace."""
    path = _SQL_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f'SQL file not found: {path}')
    return path.read_text(encoding='utf-8').strip()
