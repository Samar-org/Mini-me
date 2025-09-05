"""initial schema with RLS"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20250823222243"
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    # Tenants & Users
    op.create_table('tenant',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('name', sa.String(), nullable=False, unique=True),
        sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
    )
    op.create_table('user',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), sa.ForeignKey('tenant.id'), nullable=False, index=True),
        sa.Column('email', sa.String(), nullable=False, unique=True),
        sa.Column('hashed_password', sa.String(), nullable=False),
        sa.Column('role', sa.String(), nullable=False, server_default='operator'),
        sa.Column('active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
    )
    # Core tables
    op.create_table('product',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), nullable=False, index=True),
        sa.Column('sku', sa.String(), nullable=False),
        sa.Column('upc', sa.String(), nullable=True),
        sa.Column('asin', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('brand', sa.String(), nullable=True),
        sa.Column('category', sa.String(), nullable=True),
        sa.Column('condition', sa.String(), nullable=True),
        sa.Column('cost', sa.Numeric(12,2), nullable=True),
        sa.Column('msrp', sa.Numeric(12,2), nullable=True),
        sa.Column('attributes', sa.JSON(), nullable=True),
    )
    op.create_table('inventory',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), nullable=False, index=True),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('location', sa.String(), nullable=False, server_default='MAIN'),
        sa.Column('bin_code', sa.String(), nullable=True),
        sa.Column('qty_on_hand', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('qty_reserved', sa.Integer(), nullable=False, server_default='0'),
    )
    op.create_table('listing',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), nullable=False, index=True),
        sa.Column('product_id', sa.String(), sa.ForeignKey('product.id'), nullable=False),
        sa.Column('channel', sa.String(), nullable=False),
        sa.Column('remote_id', sa.String(), nullable=True),
        sa.Column('status', sa.String(), nullable=False, server_default='UNPUBLISHED'),
        sa.Column('price', sa.Numeric(12,2), nullable=True),
        sa.Column('currency', sa.String(), nullable=True, server_default='USD'),
        sa.Column('listing_url', sa.String(), nullable=True),
        sa.Column('last_synced_at', sa.DateTime(), nullable=True),
    )
    op.create_table('order',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), nullable=False, index=True),
        sa.Column('channel', sa.String(), nullable=False),
        sa.Column('order_no', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False, server_default='PENDING'),
        sa.Column('totals', sa.JSON(), nullable=True),
        sa.Column('placed_at', sa.DateTime(), nullable=True),
        sa.Column('customer', sa.JSON(), nullable=True),
    )
    op.create_table('outbox',
        sa.Column('id', sa.String(), primary_key=True),
        sa.Column('tenant_id', sa.String(), nullable=False, index=True),
        sa.Column('topic', sa.String(), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=False),
        sa.Column('published_at', sa.DateTime(), nullable=True),
    )

    # RLS policy: set app.tenant_id in session; restrict all tables that have tenant_id
    for tbl in ['product','inventory','listing','order','outbox']:
        op.execute(f'ALTER TABLE "{tbl}" ENABLE ROW LEVEL SECURITY;')
        op.execute(f"""
            CREATE POLICY {tbl}_tenant_isolation ON "{tbl}"
            USING (tenant_id::text = current_setting('app.tenant_id', true));
        """)

def downgrade():
    for tbl in ['outbox','order','listing','inventory','product','user','tenant']:
        op.execute(f'DROP TABLE IF EXISTS "{tbl}" CASCADE;')
