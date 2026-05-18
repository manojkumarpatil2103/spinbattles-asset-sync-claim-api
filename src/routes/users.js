const express = require('express');
const { pool, withTransaction } = require('../infra/db');
const { enqueueJob } = require('../infra/workerTransport');
const {
  userIdParam,
  claimIdParam,
  createUserBody,
  creditsBody,
  claimBody,
  validationError,
} = require('../schemas/users');

const router = express.Router();

router.post('/users', async (req, res, next) => {
  try {
    const parsed = createUserBody.safeParse(req.body);
    if (!parsed.success) {
      throw validationError(parsed.error);
    }
    const { handle } = parsed.data;

    const inserted = await pool.query(
      `insert into users (handle) values ($1)
       on conflict (handle) do nothing
       returning id, handle, created_at`,
      [handle]
    );
    if (inserted.rowCount > 0) {
      res.status(201).json(inserted.rows[0]);
      return;
    }
    const existing = await pool.query(
      `select id, handle, created_at from users where handle = $1`,
      [handle]
    );
    res.status(200).json(existing.rows[0]);
  } catch (e) {
    next(e);
  }
});

router.get('/users/:userId/credits', async (req, res, next) => {
  try {
    const p = userIdParam.safeParse(req.params);
    if (!p.success) {
      throw validationError(p.error);
    }
    const { userId } = p.data;

    const r = await pool.query(
      `select coalesce(sum(delta), 0)::int as balance
       from credits_ledger
       where user_id = $1`,
      [userId]
    );
    res.json({ user_id: userId, balance: r.rows[0].balance });
  } catch (e) {
    next(e);
  }
});

router.post('/users/:userId/credits', async (req, res, next) => {
  const pParams = userIdParam.safeParse(req.params);
  if (!pParams.success) {
    return next(validationError(pParams.error));
  }
  const { userId } = pParams.data;

  const parsed = creditsBody.safeParse(req.body);
  if (!parsed.success) {
    return next(validationError(parsed.error));
  }
  const { delta, reason, idempotency_key: idem } = parsed.data;

  try {
    const row = await withTransaction(async (client) => {
      const u = await client.query('select id from users where id = $1', [userId]);
      if (u.rowCount === 0) {
        const e = new Error('User not found');
        e.status = 404;
        throw e;
      }

      if (idem) {
        const existing = await client.query(
          `select id, delta, reason, created_at
           from credits_ledger
           where idempotency_key = $1`,
          [idem]
        );
        if (existing.rowCount > 0) {
          return { idempotent: true, ledger: existing.rows[0] };
        }
      }

      const ins = await client.query(
        `insert into credits_ledger (user_id, delta, reason, idempotency_key)
         values ($1, $2, $3, $4)
         returning id, user_id, delta, reason, idempotency_key, created_at`,
        [userId, delta, reason, idem || null]
      );
      return { idempotent: false, ledger: ins.rows[0] };
    });

    res.status(row.idempotent ? 200 : 201).json(row.ledger);
  } catch (e) {
    if (e.code === '23505' && idem) {
      const existing = await pool.query(
        `select id, user_id, delta, reason, idempotency_key, created_at
         from credits_ledger
         where idempotency_key = $1`,
        [idem]
      );
      if (existing.rowCount > 0) {
        res.status(200).json(existing.rows[0]);
        return;
      }
    }
    next(e);
  }
});

router.get('/users/:userId/inventory', async (req, res, next) => {
  try {
    const p = userIdParam.safeParse(req.params);
    if (!p.success) {
      throw validationError(p.error);
    }
    const { userId } = p.data;

    const r = await pool.query(
      `select id, sku, quantity, state, source_tx_hash, created_at, updated_at
       from inventory_items
       where user_id = $1
       order by created_at asc`,
      [userId]
    );
    res.json({ user_id: userId, items: r.rows });
  } catch (e) {
    next(e);
  }
});

router.get('/users/:userId/claims/:claimId', async (req, res, next) => {
  try {
    const p = claimIdParam.safeParse(req.params);
    if (!p.success) {
      throw validationError(p.error);
    }
    const { userId, claimId } = p.data;

    const r = await pool.query(
      `select id, user_id, sku, tx_hash, status, failure_reason, created_at, updated_at
       from claim_intents
       where id = $1 and user_id = $2`,
      [claimId, userId]
    );
    if (r.rowCount === 0) {
      res.status(404).json({ error: 'Claim not found' });
      return;
    }
    res.json({ claim: r.rows[0] });
  } catch (e) {
    next(e);
  }
});

router.post('/users/:userId/claims', async (req, res, next) => {
  try {
    const pParams = userIdParam.safeParse(req.params);
    if (!pParams.success) {
      throw validationError(pParams.error);
    }
    const { userId } = pParams.data;

    const parsed = claimBody.safeParse(req.body);
    if (!parsed.success) {
      throw validationError(parsed.error);
    }
    const { sku, tx_hash: txHash } = parsed.data;

    const result = await withTransaction(async (client) => {
      const u = await client.query('select id from users where id = $1', [userId]);
      if (u.rowCount === 0) {
        const e = new Error('User not found');
        e.status = 404;
        throw e;
      }

      const dup = await client.query('select id, status from claim_intents where tx_hash = $1', [
        txHash,
      ]);
      if (dup.rowCount > 0) {
        const e = new Error('Duplicate claim for tx_hash');
        e.status = 409;
        e.details = dup.rows[0];
        throw e;
      }

      const ins = await client.query(
        `insert into claim_intents (user_id, sku, tx_hash, status)
         values ($1, $2, $3, 'queued')
         returning id, user_id, sku, tx_hash, status, created_at`,
        [userId, sku, txHash]
      );

      return ins.rows[0];
    });

    const requestId = req.id || req.headers['x-request-id'];
    const job = {
      type: 'PROCESS_CLAIM',
      claimId: result.id,
      requestId,
      userId: result.user_id,
      sku: result.sku,
      txHash: result.tx_hash,
    };

    req.log.info(
      {
        requestId,
        claimId: result.id,
        userId: result.user_id,
        sku: result.sku,
        txHash: result.tx_hash,
        status: result.status,
      },
      'claim intent created'
    );

    enqueueJob(job);

    req.log.info(
      {
        requestId,
        claimId: result.id,
        userId: result.user_id,
        sku: result.sku,
        txHash: result.tx_hash,
        jobType: job.type,
      },
      'claim job enqueued'
    );

    res.status(202).json({
      claim: result,
      message: 'Claim queued for asynchronous verification (background worker).',
    });
  } catch (e) {
    if (e.code === '23505') {
      res.status(409).json({ error: 'Duplicate claim for tx_hash' });
      return;
    }
    next(e);
  }
});

module.exports = router;
