"""empty message

Revision ID: 1224e1d6a73b
Revises: 1cb52f9eef83
Create Date: 2019-10-19 11:22:58.036656

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1224e1d6a73b'
down_revision = '1cb52f9eef83'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('childrens_corners',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('ta_id', sa.Integer(), nullable=True),
    sa.Column('district_id', sa.Integer(), nullable=True),
    sa.Column('name', sa.String(length=64), nullable=True),
    sa.ForeignKeyConstraint(['district_id'], ['locations.id'], ),
    sa.ForeignKeyConstraint(['ta_id'], ['traditional_authorities.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_childrens_corners_name'), 'childrens_corners', ['name'], unique=False)
    op.drop_index('ix_childrens_corner_name', table_name='childrens_corner')
    op.drop_table('childrens_corner')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('childrens_corner',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('ta_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('district_id', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('name', sa.VARCHAR(length=64), autoincrement=False, nullable=True),
    sa.ForeignKeyConstraint(['district_id'], ['locations.id'], name='childrens_corner_district_id_fkey'),
    sa.ForeignKeyConstraint(['ta_id'], ['traditional_authorities.id'], name='childrens_corner_ta_id_fkey'),
    sa.PrimaryKeyConstraint('id', name='childrens_corner_pkey')
    )
    op.create_index('ix_childrens_corner_name', 'childrens_corner', ['name'], unique=False)
    op.drop_index(op.f('ix_childrens_corners_name'), table_name='childrens_corners')
    op.drop_table('childrens_corners')
    # ### end Alembic commands ###
