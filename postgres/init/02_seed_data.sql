-- =============================================================================
-- Seed dim_asset with the default coin set so foreign-keyed joins work even
-- before the first event arrives. The producer also upserts these rows on the
-- fly, so reseeding is harmless.
-- =============================================================================

\connect crypto

INSERT INTO dim_asset (asset_id, symbol, name, market_cap_rank) VALUES
    ('bitcoin',     'BTC',  'Bitcoin',     1),
    ('ethereum',    'ETH',  'Ethereum',    2),
    ('solana',      'SOL',  'Solana',      3),
    ('cardano',     'ADA',  'Cardano',     4),
    ('polkadot',    'DOT',  'Polkadot',    5),
    ('ripple',      'XRP',  'XRP',         6),
    ('dogecoin',    'DOGE', 'Dogecoin',    7),
    ('avalanche-2', 'AVAX', 'Avalanche',   8),
    ('chainlink',   'LINK', 'Chainlink',   9),
    ('polygon',     'MATIC','Polygon',    10)
ON CONFLICT (asset_id) DO NOTHING;
