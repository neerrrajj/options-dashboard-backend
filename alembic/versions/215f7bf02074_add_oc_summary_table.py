"""Add oc_summary table

Revision ID: 215f7bf02074
Revises: ecc8f294a9ab
Create Date: 2025-06-20 22:41:50.156537

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '215f7bf02074'
down_revision: Union[str, Sequence[str], None] = 'ecc8f294a9ab'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
