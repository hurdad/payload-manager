async function apiFetch(path, options = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });
  if (!res.ok) {
    let msg = `HTTP ${res.status}`;
    try { const e = await res.json(); msg = e.message || msg; } catch {}
    throw new Error(msg);
  }
  if (res.status === 204) return null;
  return res.json();
}

export const api = {
  // Payload Catalog
  listPayloads: (tier) =>
    apiFetch(`/v1/payloads${tier ? `?tierFilter=${tier}` : ''}`),

  deletePayload: (id, force = false) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}${force ? '?force=true' : ''}`, { method: 'DELETE' }),

  spill: (ids) =>
    apiFetch('/v1/payloads/spill', {
      method: 'POST',
      body: JSON.stringify({ ids: ids.map((v) => ({ value: v })), policy: 'SPILL_POLICY_BLOCKING', fsync: true }),
    }),

  promote: (id, targetTier = 'TIER_RAM') =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/promote`, {
      method: 'POST',
      body: JSON.stringify({ targetTier, policy: 'PROMOTION_POLICY_BLOCKING' }),
    }),

  pin: (id, durationMs = 0) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/pin`, {
      method: 'POST',
      body: JSON.stringify({ durationMs: String(durationMs) }),
    }),

  unpin: (id) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/pin`, { method: 'DELETE' }),

  prefetch: (id, targetTier = 'TIER_RAM') =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/prefetch`, {
      method: 'POST',
      body: JSON.stringify({ targetTier }),
    }),

  snapshot: (id) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/snapshot`),

  lineage: (id, upstream = false, maxDepth = 10) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/lineage?upstream=${upstream}&maxDepth=${maxDepth}`),

  updateMetadata: (id, raw, schema = '') =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/metadata`, {
      method: 'PUT',
      body: JSON.stringify({
        mode: 'METADATA_UPDATE_MODE_REPLACE',
        metadata: { data: btoa(raw), schema },
      }),
    }),

  appendMetadataEvent: (id, raw, source = '', version = '') =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/metadata/events`, {
      method: 'POST',
      body: JSON.stringify({
        metadata: { data: btoa(raw), schema: '' },
        source,
        version,
      }),
    }),

  // Payload Data
  resolveSnapshot: (id) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/snapshot`),

  acquireLease: (id, minTier = 'TIER_DISK', durationMs = 30000) =>
    apiFetch(`/v1/payloads/${encodeURIComponent(id)}/lease`, {
      method: 'POST',
      body: JSON.stringify({
        mode: 'LEASE_MODE_READ',
        minTier,
        promotionPolicy: 'PROMOTION_POLICY_BLOCKING',
        minLeaseDurationMs: String(durationMs),
      }),
    }),

  releaseLease: (leaseId) =>
    apiFetch(`/v1/leases/${encodeURIComponent(leaseId)}`, { method: 'DELETE' }),

  // Admin
  stats: () => apiFetch('/v1/admin/stats'),

  // Streams
  createStream: (namespace, name, retentionMaxEntries = 0, retentionMaxAgeSec = 0) =>
    apiFetch('/v1/streams', {
      method: 'POST',
      body: JSON.stringify({
        stream: { namespace, name },
        retentionMaxEntries: String(retentionMaxEntries),
        retentionMaxAgeSec: String(retentionMaxAgeSec),
      }),
    }),

  deleteStream: (namespace, name) =>
    apiFetch(`/v1/streams/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`, { method: 'DELETE' }),

  readStream: (namespace, name, startOffset = 0, maxEntries = 50) =>
    apiFetch(
      `/v1/streams/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/entries?startOffset=${startOffset}&maxEntries=${maxEntries}`
    ),

  appendStream: (namespace, name, payloadIds) =>
    apiFetch(`/v1/streams/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/entries`, {
      method: 'POST',
      body: JSON.stringify({
        stream: { namespace, name },
        items: payloadIds.map((v) => ({
          payloadId: { value: v },
          eventTime: new Date().toISOString(),
        })),
      }),
    }),

  getCommitted: (namespace, name, consumerGroup) =>
    apiFetch(
      `/v1/streams/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/committed?consumerGroup=${encodeURIComponent(consumerGroup)}`
    ),

  commitOffset: (namespace, name, consumerGroup, offset) =>
    apiFetch(`/v1/streams/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/commit`, {
      method: 'POST',
      body: JSON.stringify({ stream: { namespace, name }, consumerGroup, offset: String(offset) }),
    }),
};
