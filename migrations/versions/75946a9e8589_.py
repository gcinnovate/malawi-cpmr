"""empty message

Revision ID: 75946a9e8589
Revises: 1224e1d6a73b
Create Date: 2019-10-21 12:12:34.273205

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '75946a9e8589'
down_revision = '1224e1d6a73b'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('flow_data', sa.Column('children_corner', sa.Integer(), nullable=True))
    op.add_column('flow_data', sa.Column('cvsu', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'flow_data', 'childrens_corners', ['children_corner'], ['id'])
    op.create_foreign_key(None, 'flow_data', 'community_victim_support_units', ['cvsu'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'flow_data', type_='foreignkey')
    op.drop_constraint(None, 'flow_data', type_='foreignkey')
    op.drop_column('flow_data', 'cvsu')
    op.drop_column('flow_data', 'children_corner')
    # ### end Alembic commands ###