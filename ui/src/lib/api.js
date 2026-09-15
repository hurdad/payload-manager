/** Convert standard base64 to URL-safe base64 for use in URL path segments.
 *  Keeps = padding — grpc-gateway URL encoding requires it. */
export function toURLSafe(id) {
  return id.replace(/\+/g, '-').replace(/\//g, '_');
}

/** Base64-encode a string as UTF-8.
 *
 *  btoa() alone is wrong for anything outside ASCII, and wrong in two different
 *  ways. For U+0080-U+00FF it succeeds and encodes the Latin-1 byte: "café"
 *  became `Y2Fm6Q==` (63 61 66 e9) where UTF-8 is `Y2Fmw6k=` (63 61 66 c3 a9),
 *  so the catalog silently stored bytes that are not valid UTF-8. Above U+00FF
 *  it throws InvalidCharacterError, which surfaced as a bare
 *  "Error: Invalid character" in the metadata editor — the case you hit by
 *  pasting text containing an em-dash or a curly quote, never mind CJK.
 *
 *  TextEncoder always produces UTF-8. The chunking is because
 *  String.fromCharCode(...bytes) spreads one argument per byte, which blows the
 *  argument limit on metadata of any real size. */
export function base64FromUtf8(text) {
  const bytes = new TextEncoder().encode(text);
  const CHUNK = 0x8000;
  let binary = '';
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

/** Inverse of base64FromUtf8, for metadata read back off the wire. */
export function utf8FromBase64(b64) {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return new TextDecoder().decode(bytes);
}

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
  listPayloads: async (tier, pageSize = 50, pageToken = '') => {
    const params = new URLSearchParams();
    if (tier) params.set('tierFilter', tier);
    if (pageSize !== 50) params.set('pageSize', String(pageSize));
    if (pageToken) params.set('pageToken', pageToken);
    const qs = params.toString();
    const res = await fetch(`/v1/payloads${qs ? `?${qs}` : ''}`, {
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) {
      let msg = `HTTP ${res.status}`;
      try { const e = await res.json(); msg = e.message || msg; } catch {}
      throw new Error(msg);
    }
    const data = await res.json();
    const dateHeader = res.headers.get('Date');
    const serverNow = dateHeader ? new Date(dateHeader).getTime() : null;
    return { ...data, serverNow };
  },

  deletePayload: (id, force = false) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}${force ? '?force=true' : ''}`, { method: 'DELETE' }),

  spill: async (ids, targetTier) => {
    const body = { ids: ids.map((v) => ({ value: v })), policy: 'SPILL_POLICY_BLOCKING', fsync: true };
    if (targetTier) body.targetTier = targetTier;
    const res = await apiFetch('/v1/payloads/spill', { method: 'POST', body: JSON.stringify(body) });
    const failed = (res?.results ?? []).filter(r => !r.ok);
    if (failed.length > 0) throw new Error(failed.map(r => r.errorMessage).join('; '));
    return res;
  },

  promote: (id, targetTier = 'TIER_RAM') =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/promote`, {
      method: 'POST',
      body: JSON.stringify({ targetTier, policy: 'PROMOTION_POLICY_BLOCKING' }),
    }),

  pin: (id, durationMs = 0) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/pin`, {
      method: 'POST',
      body: JSON.stringify({ durationMs: String(durationMs) }),
    }),

  unpin: (id) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/pin`, { method: 'DELETE' }),

  prefetch: (id, targetTier = 'TIER_RAM') =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/prefetch`, {
      method: 'POST',
      body: JSON.stringify({ targetTier }),
    }),

  snapshot: (id) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/snapshot`),

  lineage: (id, upstream = false, maxDepth = 10) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/lineage?upstream=${upstream}&maxDepth=${maxDepth}`),

  updateMetadata: (id, raw, schema = '') =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/metadata`, {
      method: 'PUT',
      body: JSON.stringify({
        mode: 'METADATA_UPDATE_MODE_REPLACE',
        metadata: { data: base64FromUtf8(raw), schema },
      }),
    }),

  appendMetadataEvent: (id, raw, source = '', version = '') =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/metadata/events`, {
      method: 'POST',
      body: JSON.stringify({
        metadata: { data: base64FromUtf8(raw), schema: '' },
        source,
        version,
      }),
    }),

  // Payload Data
  resolveSnapshot: (id) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/snapshot`),

  acquireLease: (id, minTier = 'TIER_DISK', durationMs = 30000) =>
    apiFetch(`/v1/payloads/${toURLSafe(id)}/lease`, {
      method: 'POST',
      body: JSON.stringify({
        mode: 'LEASE_MODE_READ',
        minTier,
        promotionPolicy: 'PROMOTION_POLICY_BLOCKING',
        minLeaseDurationMs: String(durationMs),
      }),
    }),

  releaseLease: (leaseId) =>
    apiFetch(`/v1/leases/${toURLSafe(leaseId)}`, { method: 'DELETE' }),

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
