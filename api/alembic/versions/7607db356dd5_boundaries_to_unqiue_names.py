"""Boundaries to unqiue names

Revision ID: 7607db356dd5
Revises: 49b733ecc873
Create Date: 2022-12-19 11:16:18.294685

"""
from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision = '7607db356dd5'
down_revision = '49b733ecc873'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('boundaries', 'name',
               existing_type=sa.VARCHAR(),
               nullable=False)
    op.alter_column('boundaries', 'name_long',
               existing_type=sa.VARCHAR(),
               nullable=False)
    op.alter_column('boundaries', 'admin_level',
               existing_type=sa.VARCHAR(),
               nullable=False)
    op.alter_column('boundaries', 'geometry',
               existing_type=geoalchemy2.types.Geometry(geometry_type='MULTIPOLYGON', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', _spatial_index_reflected=True),
               nullable=False)
    op.drop_index('ix_boundaries_name', table_name='boundaries')
    op.create_index(op.f('ix_boundaries_name'), 'boundaries', ['name'], unique=True)
    op.drop_index('ix_boundaries_name_long', table_name='boundaries')
    op.create_index(op.f('ix_boundaries_name_long'), 'boundaries', ['name_long'], unique=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_boundaries_name_long'), table_name='boundaries')
    op.create_index('ix_boundaries_name_long', 'boundaries', ['name_long'], unique=False)
    op.drop_index(op.f('ix_boundaries_name'), table_name='boundaries')
    op.create_index('ix_boundaries_name', 'boundaries', ['name'], unique=False)
    op.alter_column('boundaries', 'geometry',
               existing_type=geoalchemy2.types.Geometry(geometry_type='MULTIPOLYGON', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', _spatial_index_reflected=True),
               nullable=True)
    op.alter_column('boundaries', 'admin_level',
               existing_type=sa.VARCHAR(),
               nullable=True)
    op.alter_column('boundaries', 'name_long',
               existing_type=sa.VARCHAR(),
               nullable=True)
    op.alter_column('boundaries', 'name',
               existing_type=sa.VARCHAR(),
               nullable=True)
    # ### end Alembic commands ###
