-- SpinBattles Engineering Assessment — starter schema (do not treat as production-ready)
-- gen_random_uuid() is built-in from PostgreSQL 13+ (no pgcrypto needed; compatible with embedded PGlite).

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  handle TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS credits_ledger (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  delta INTEGER NOT NULL,
  reason TEXT NOT NULL,
  idempotency_key TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity >= 0),
  state TEXT NOT NULL DEFAULT 'confirmed' CHECK (state IN ('pending', 'confirmed', 'failed')),
  source_tx_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, sku, source_tx_hash)
);

CREATE TABLE IF NOT EXISTS claim_intents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
  sku TEXT NOT NULL,
  tx_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'processing', 'confirmed', 'failed')),
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tx_hash)
);

CREATE INDEX IF NOT EXISTS idx_claim_intents_user ON claim_intents (user_id);
CREATE INDEX IF NOT EXISTS idx_inventory_user ON inventory_items (user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_source_tx_hash_unique
  ON inventory_items (source_tx_hash)
  WHERE source_tx_hash IS NOT NULL;
  -- source_tx_hash can only create one inventory grant

INSERT INTO users (handle)
VALUES ('demo_player')
ON CONFLICT (handle) DO NOTHING;
-- on conflict do update set quantity = quantity + 1 
-- (before insert,  user exists without modifying existing data.)
