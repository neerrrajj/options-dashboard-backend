"""add_unique_constraints_for_upsert

Revision ID: 20250221
Revises: 215f7bf02074
Create Date: 2025-02-21 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20250221'
down_revision: Union[str, Sequence[str], None] = '215f7bf02074'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add unique constraints for UPSERT operations."""
    # OCMinuteSnapshot - unique constraint for upsert
    op.create_unique_constraint(
        'uq_snapshot_lookup',
        'oc_minute_snapshots',
        ['instrument', 'expiry', 'ist_minute', 'strike']
    )
    
    # OCSummary - unique constraint for upsert
    op.create_unique_constraint(
        'uq_summary_lookup',
        'oc_summary',
        ['instrument', 'expiry', 'ist_minute']
    )
    
    # HistoricalOCSnapshot - unique constraint for upsert
    op.create_unique_constraint(
        'uq_hist_snapshot_lookup',
        'historical_oc_snapshots',
        ['instrument', 'expiry', 'ist_minute', 'strike']
    )
    
    # HistoricalOCSummary - unique constraint for upsert
    op.create_unique_constraint(
        'uq_hist_summary_lookup',
        'historical_oc_summary',
        ['instrument', 'expiry', 'ist_minute']
    )


def downgrade() -> None:
    """Remove unique constraints."""
    op.drop_constraint('uq_hist_summary_lookup', 'historical_oc_summary', type_='unique')
    op.drop_constraint('uq_hist_snapshot_lookup', 'historical_oc_snapshots', type_='unique')
    op.drop_constraint('uq_summary_lookup', 'oc_summary', type_='unique')
    op.drop_constraint('uq_snapshot_lookup', 'oc_minute_snapshots', type_='unique')
