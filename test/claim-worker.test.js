process.env.LOG_LEVEL = 'fatal';
process.env.MOCK_CHAIN = 'true';

const assert = require('node:assert/strict');
const test = require('node:test');
const { randomUUID } = require('node:crypto');

const { pool } = require('../src/infra/db');
const { processClaimMessage } = require('../src/worker');

function txHash() {
  return `0x${Buffer.from(randomUUID().replaceAll('-', '').padEnd(64, '0'), 'hex').toString('hex')}`;
}

async function resetDb() {
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_source_tx_hash_unique
      ON inventory_items (source_tx_hash)
      WHERE source_tx_hash IS NOT NULL
  `);
  await pool.query('delete from inventory_items');
  await pool.query('delete from claim_intents');
  await pool.query('delete from credits_ledger');
  await pool.query("delete from users where handle like 'test_%'");
}

async function createUser() {
  const handle = `test_${randomUUID()}`;
  const result = await pool.query(
    `insert into users (handle)
     values ($1)
     returning id`,
    [handle]
  );
  return result.rows[0].id;
}

async function createClaim(userId, tx, sku = 'ASSET_CHEST') {
  const result = await pool.query(
    `insert into claim_intents (user_id, sku, tx_hash, status)
     values ($1, $2, $3, 'queued')
     returning id`,
    [userId, sku, tx]
  );
  return result.rows[0].id;
}

test.beforeEach(resetDb);

test.after(async () => {
  await resetDb();
  await pool.end();
});

test('processing the same claim message twice grants inventory once', async () => {
  const userId = await createUser();
  const tx = txHash();
  const claimId = await createClaim(userId, tx);
  const job = JSON.stringify({ type: 'PROCESS_CLAIM', claimId, requestId: 'test-request' });

  await processClaimMessage(job);
  await processClaimMessage(job);

  const inventory = await pool.query(
    `select sku, quantity, source_tx_hash
     from inventory_items
     where user_id = $1`,
    [userId]
  );
  const claim = await pool.query('select status from claim_intents where id = $1', [claimId]);

  assert.equal(inventory.rowCount, 1);
  assert.equal(inventory.rows[0].quantity, 1);
  assert.equal(inventory.rows[0].source_tx_hash, tx);
  assert.equal(claim.rows[0].status, 'confirmed');
});

test('existing inventory for a tx_hash is not incremented by a retried claim finalize', async () => {
  const userId = await createUser();
  const tx = txHash();
  const claimId = await createClaim(userId, tx);

  await pool.query(
    `insert into inventory_items (user_id, sku, quantity, state, source_tx_hash)
     values ($1, 'ASSET_CHEST', 1, 'confirmed', $2)`,
    [userId, tx]
  );

  await processClaimMessage(JSON.stringify({ type: 'PROCESS_CLAIM', claimId }));

  const inventory = await pool.query(
    `select quantity
     from inventory_items
     where user_id = $1 and source_tx_hash = $2`,
    [userId, tx]
  );
  const claim = await pool.query('select status from claim_intents where id = $1', [claimId]);

  assert.equal(inventory.rowCount, 1);
  assert.equal(inventory.rows[0].quantity, 1);
  assert.equal(claim.rows[0].status, 'confirmed');
});

test('duplicate claim_intents cannot be created for the same tx_hash', async () => {
  const userId = await createUser();
  const tx = txHash();

  await createClaim(userId, tx, 'ASSET_CHEST');

  await assert.rejects(
    () => createClaim(userId, tx, 'ASSET_KEY'),
    (error) => error && error.code === '23505'
  );
});
