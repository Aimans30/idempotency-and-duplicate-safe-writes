const crypto = require('crypto');
const { db } = require('./db');

const OPERATION = 'POST:/incidents';
const IDEMPOTENCY_TTL_HOURS = 24;

/**
 * Build a deterministic representation of the request.
 *
 * Only the fields that define the logical incident are included.
 * Object property order does not affect the resulting hash.
 */
function canonicalize(value) {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalize).join(',')}]`;
  }

  if (value && typeof value === 'object') {
    return `{${Object.keys(value)
      .sort()
      .map(
        key =>
          `${JSON.stringify(key)}:${canonicalize(value[key])}`
      )
      .join(',')}}`;
  }

  return JSON.stringify(value);
}

function hashRequest(body) {
  const canonical = canonicalize({
    title: body.title,
    severity: body.severity,
    serviceId: body.serviceId
  });

  return crypto
    .createHash('sha256')
    .update(canonical)
    .digest('hex');
}

/**
 * Return a previously completed response.
 */
function replayResponse(res, record) {
  res.set('Idempotent-Replayed', 'true');

  return res
    .status(record.response_status)
    .json(record.response_body);
}

/**
 * Claim an idempotency key.
 *
 * The UNIQUE constraint on (tenant_id, operation, key) is the
 * concurrency boundary. Only one request can successfully insert
 * the processing record.
 */
async function claimIdempotencyKey(t, {
  tenantId,
  key,
  requestHash
}) {
  const expiresAt = new Date(
    Date.now() + IDEMPOTENCY_TTL_HOURS * 60 * 60 * 1000
  );

  const inserted = await t.oneOrNone(
    `
      INSERT INTO idempotency_keys (
        tenant_id,
        operation,
        key,
        request_hash,
        state,
        expires_at
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        'processing',
        $5
      )
      ON CONFLICT (tenant_id, operation, key)
      DO NOTHING
      RETURNING *
    `,
    [
      tenantId,
      OPERATION,
      key,
      requestHash,
      expiresAt
    ]
  );

  if (inserted) {
    return {
      type: 'claimed',
      record: inserted
    };
  }

  /*
   * If another transaction is currently inserting/updating the same
   * unique key, PostgreSQL makes the INSERT wait for that transaction.
   *
   * Therefore this SELECT sees the committed state of the winner.
   */
  const existing = await t.one(
    `
      SELECT *
      FROM idempotency_keys
      WHERE tenant_id = $1
        AND operation = $2
        AND key = $3
      FOR UPDATE
    `,
    [tenantId, OPERATION, key]
  );

  /*
   * Exercise expiry:
   * an expired idempotency record may be reclaimed.
   */
  if (existing.expires_at <= new Date()) {
    const renewed = await t.one(
      `
        UPDATE idempotency_keys
        SET request_hash = $1,
            state = 'processing',
            response_status = NULL,
            response_body = NULL,
            expires_at = $2,
            updated_at = now()
        WHERE id = $3
        RETURNING *
      `,
      [
        requestHash,
        expiresAt,
        existing.id
      ]
    );

    return {
      type: 'claimed',
      record: renewed
    };
  }

  /*
   * Same idempotency key but different logical request.
   */
  if (existing.request_hash !== requestHash) {
    return {
      type: 'conflict',
      record: existing
    };
  }

  /*
   * Same key and same request. Replay if the original operation
   * completed successfully.
   */
  if (existing.state === 'completed') {
    return {
      type: 'replay',
      record: existing
    };
  }

  /*
   * A request that is still processing must not execute the
   * operation a second time.
   */
  if (existing.state === 'processing') {
    return {
      type: 'processing',
      record: existing
    };
  }

  /*
   * A previous attempt failed. The exercise contract requires
   * callers to receive the recovery signal rather than silently
   * executing the operation again.
   */
  return {
    type: 'failed',
    record: existing
  };
}

async function createIncident(req, res) {
  const key = req.get('Idempotency-Key');

  if (!key || !key.trim()) {
    return res.status(400).json({
      error: 'idempotency_key_required'
    });
  }

  const {
    title,
    severity,
    serviceId
  } = req.body;

  const requestHash = hashRequest({
    title,
    severity,
    serviceId
  });

  const tenantId = req.user.tenantId;

  /*
   * The entire idempotency claim + incident + paging job +
   * completion record is one transaction.
   */
  const result = await db.tx(async t => {
    const claim = await claimIdempotencyKey(t, {
      tenantId,
      key: key.trim(),
      requestHash
    });

    if (claim.type !== 'claimed') {
      return claim;
    }

    const incident = await t.one(
      `
        INSERT INTO incidents (
          tenant_id,
          service_id,
          title,
          severity
        )
        VALUES ($1, $2, $3, $4)
        RETURNING *
      `,
      [
        tenantId,
        serviceId,
        title,
        severity
      ]
    );

    /*
     * The paging job is deliberately created in the same
     * transaction as the incident.
     */
    await t.none(
      `
        INSERT INTO paging_jobs (
          tenant_id,
          incident_id,
          status
        )
        VALUES ($1, $2, 'pending')
      `,
      [
        tenantId,
        incident.id
      ]
    );

    /*
     * Store the exact response that the first request returned.
     * Future requests with the same key can replay it.
     */
    await t.none(
      `
        UPDATE idempotency_keys
        SET state = 'completed',
            response_status = 201,
            response_body = $1::jsonb,
            updated_at = now()
        WHERE id = $2
      `,
      [
        JSON.stringify(incident),
        claim.record.id
      ]
    );

    return {
      type: 'created',
      incident
    };
  });

  if (result.type === 'conflict') {
    return res.status(409).json({
      error: 'idempotency_key_conflict'
    });
  }

  if (result.type === 'processing') {
    return res.status(409).json({
      error: 'operation_in_progress'
    });
  }

  if (result.type === 'failed') {
    return res.status(409).json({
      error: 'prior_operation_failed'
    });
  }

  if (result.type === 'replay') {
    return replayResponse(res, result.record);
  }

  return res.status(201).json(result.incident);
}

module.exports = {
  createIncident,
  hashRequest
};