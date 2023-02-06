"""Added Boundary Table

Revision ID: 49b733ecc873
Revises: 
Create Date: 2022-12-14 12:24:35.901204

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '49b733ecc873'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('boundaries',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('name', sa.String(), nullable=True),
    sa.Column('name_long', sa.String(), nullable=True),
    sa.Column('admin_level', sa.String(), nullable=True),
    sa.Column('geometry', geoalchemy2.types.Geometry(geometry_type='MULTIPOLYGON', srid=4326, from_text='ST_GeomFromEWKT', name='geometry'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_boundaries_admin_level'), 'boundaries', ['admin_level'], unique=False)
    op.create_index(op.f('ix_boundaries_id'), 'boundaries', ['id'], unique=False)
    op.create_index(op.f('ix_boundaries_name'), 'boundaries', ['name'], unique=False)
    op.create_index(op.f('ix_boundaries_name_long'), 'boundaries', ['name_long'], unique=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_boundaries_name_long'), table_name='boundaries')
    op.drop_index(op.f('ix_boundaries_name'), table_name='boundaries')
    op.drop_index(op.f('ix_boundaries_id'), table_name='boundaries')
    op.drop_index(op.f('ix_boundaries_admin_level'), table_name='boundaries')
    op.drop_index('idx_boundaries_geometry', table_name='boundaries', postgresql_using='gist')
    op.drop_table('boundaries')
    # ### end Alembic commands ###