/**
 * Background worker: consumes jobs from the API push socket (`src/infra/workerTransport.js`).
 */
require('dotenv').config();

const { createWorkerPullSocket } = require('./infra/workerTransport');
const { ethers } = require('ethers');
const pRetry = require('p-retry');
const { AbortError } = pRetry;

const config = require('./config');
const { pool, withTransaction } = require('./infra/db');
const { createLogger } = require('./infra/logger');

const log = createLogger('spinbattles-worker');

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function verifyOnChainTx(txHash, jobLog) {
  if (config.mockChain || !config.rpcUrl) {
    return { ok: true, mode: 'mock' };
  }

  const provider = new ethers.JsonRpcProvider(config.rpcUrl);

  try {
    await pRetry(
      async () => {
        const r = await provider.getTransactionReceipt(txHash);
        if (!r) {
          jobLog.debug({ txHash }, 'receipt not available yet');
          throw new Error('pending');
        }
        if (r.status !== 1) {
          throw new AbortError(new Error('tx_reverted'));
        }
        return r;
      },
      {
        retries: config.rpcReceiptRetries,
        minTimeout: 500,
        maxTimeout: 4000,
        factor: 2,
        onFailedAttempt: (error) => {
          if (error instanceof AbortError) return;
          jobLog.warn(
            { txHash, attemptNumber: error.attemptNumber, retriesLeft: error.retriesLeft },
            'RPC receipt fetch attempt'
          );
        },
      }
    );
    return { ok: true, mode: 'live' };
  } catch (e) {
    if (e && e.message === 'tx_reverted') {
      return { ok: false, mode: 'live', reason: 'tx_reverted' };
    }
    if (e && e.message === 'pending') {
      return { ok: false, mode: 'live', reason: 'receipt_not_found' };
    }
    jobLog.error({ txHash, err: e.message }, 'RPC receipt verification failed');
    return { ok: false, mode: 'live', reason: `rpc_error:${e.message}` };
  }
}

async function processClaimMessage(raw) {
  const rawStr = String(raw).trim();
  if (rawStr === 'task-1') {
    log.debug('Warm-up message from API job socket bootstrap (starter pattern)');
    return;
  }

  let job;
  try {
    job = JSON.parse(rawStr);
  } catch {
    log.warn({ raw: rawStr.slice(0, 200) }, 'Ignoring non-JSON queue message');
    return;
  }

  if (!job || job.type !== 'PROCESS_CLAIM' || !job.claimId) {
    log.warn({ job }, 'Ignoring unknown job');
    return;
  }

  const claimId = job.claimId;
  const jobLog = log.child({
    claimId,
    requestId: job.requestId,
    userId: job.userId,
    sku: job.sku,
    txHash: job.txHash,
    jobType: job.type,
  });

  jobLog.info('Received claim job');

  const claimRow = await pool.query(
    `select id, user_id, sku, tx_hash, status
     from claim_intents
     where id = $1`,
    [claimId]
  );
  if (claimRow.rowCount === 0) {
    jobLog.warn('Claim not found');
    return;
  }

  const claim = claimRow.rows[0];
  if (claim.status === 'confirmed' || claim.status === 'failed') {
    jobLog.info({ status: claim.status }, 'Skipping terminal claim');
    return;
  }

  let claimForVerification;
  await withTransaction(async (client) => {
    const locked = await client.query(
      `select id, status, tx_hash, sku, user_id
       from claim_intents
       where id = $1
       for update`,
      [claimId]
    );
    const c = locked.rows[0];
    if (!c || c.status === 'confirmed' || c.status === 'failed') {
      return;
    }

    await client.query(
      `update claim_intents
       set status = 'processing', updated_at = now()
       where id = $1`,
      [claimId]
    );

    claimForVerification = c;
  });

  if (!claimForVerification) {
    jobLog.info('Claim already handled by another delivery');
    return;
  }

  const claimLog = jobLog.child({
    userId: claimForVerification.user_id,
    sku: claimForVerification.sku,
    txHash: claimForVerification.tx_hash,
  });

  claimLog.info({ status: 'processing' }, 'Claim marked processing');

  let verification;
  try {
    verification = await verifyOnChainTx(claimForVerification.tx_hash, claimLog);
  } catch (e) {
    verification = { ok: false, reason: `rpc_error:${e.message}` };
  }

  claimLog.info(
    { verificationMode: verification.mode, ok: verification.ok, reason: verification.reason },
    'Claim verification finished'
  );

  await withTransaction(async (client) => {
    const locked = await client.query(
      `select id, status, tx_hash, sku, user_id
       from claim_intents
       where id = $1
       for update`,
      [claimId]
    );
    const c = locked.rows[0];
    if (!c || c.status === 'confirmed' || c.status === 'failed') {
      claimLog.info({ status: c && c.status }, 'Skipping terminal claim during finalize');
      return;
    }

    if (!verification.ok) {
      await client.query(
        `update claim_intents
         set status = 'failed',
             failure_reason = $2,
             updated_at = now()
         where id = $1`,
        [claimId, verification.reason || 'verification_failed']
      );
      claimLog.warn(
        { status: 'failed', reason: verification.reason || 'verification_failed' },
        'Claim marked failed'
      );
      return;
    }

    const granted = await client.query(
      `insert into inventory_items (user_id, sku, quantity, state, source_tx_hash)
       values ($1, $2, 1, 'confirmed', $3)
       on conflict do nothing
       returning id`,
      [c.user_id, c.sku, c.tx_hash]
    );

    await client.query(
      `update claim_intents
       set status = 'confirmed', updated_at = now()
       where id = $1`,
      [claimId]
    );

    claimLog.info(
      {
        status: 'confirmed',
        inventoryGranted: granted.rowCount > 0,
        inventoryItemId: granted.rows[0] && granted.rows[0].id,
      },
      'Claim marked confirmed'
    );
  });

  claimLog.info('Processed claim');
}

function startPullWorker() {
  const pull = createWorkerPullSocket();
  const port = config.workerSocketPort;
  const host = config.workerSocketHost;

  if (typeof pull.connect === 'function') {
    pull.connect(port, host);
  } else {
    throw new Error('Pull socket is missing connect(); check dependency version.');
  }

  pull.on('message', (msg) => {
    processClaimMessage(msg).catch((e) => {
      log.error({ err: e }, 'Claim worker error');
    });
  });

  log.info({ host, port }, 'Worker connected (pull)');
  return pull;
}

async function runWorker(options = {}) {
  const attachSignals = options.attachSignals !== false;
  const skipSleep = options.skipSleep === true;

  if (!config.databaseUrl && !config.useEmbeddedPostgres) {
    throw new Error(
      'DATABASE_URL is required for external Postgres. Omit DATABASE_URL to use embedded local Postgres (zero-config).'
    );
  }

  if (!skipSleep) {
    await sleep(750);
  }

  await pool.query('select 1');
  const pull = startPullWorker();

  const shutdown = async () => {
    try {
      pull.close();
    } catch (_) {}
    try {
      await pool.end();
    } catch (_) {}
    process.exit(0);
  };

  if (attachSignals) {
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  }

  return { pull, shutdown };
}

async function main() {
  await runWorker({ attachSignals: true });
}

const embeddedSingle = process.env.SPIN_EMBEDDED_SINGLE_PROCESS === '1';
if (require.main === module && !embeddedSingle) {
  main().catch((e) => {
    log.fatal({ err: e }, 'Worker failed to start');
    process.exit(1);
  });
}

module.exports = {
  verifyOnChainTx,
  processClaimMessage,
  startPullWorker,
  runWorker,
};
