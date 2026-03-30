/**
 * Minimal REST API helpers for test setup/teardown.
 * Called directly with fetch (no browser context needed).
 */

const BASE = process.env.BASE_URL || 'http://localhost:8080';

async function apiFetch(path: string, options: RequestInit = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers as Record<string, string> },
    ...options,
  });
  if (!res.ok) {
    let msg = `HTTP ${res.status} ${path}`;
    try { const e = await res.json(); msg = e.message || msg; } catch {}
    throw new Error(msg);
  }
  if (res.status === 204) return null;
  return res.json();
}

/** Encode a UUID string to the URL-safe base64 the API expects in paths. */
export function uuidToBase64(uuid: string): string {
  const hex = uuid.replace(/-/g, '');
  const bytes = new Uint8Array(hex.match(/.{1,2}/g)!.map(b => parseInt(b, 16)));
  let raw = '';
  bytes.forEach(b => (raw += String.fromCharCode(b)));
  return btoa(raw).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export interface PayloadIds {
  /** Raw base64 as returned by the API (may contain +//) */
  raw: string;
  /** URL-safe base64 with no padding */
  urlSafe: string;
}

/**
 * Allocate + write + commit a test payload.
 * Returns both the raw API id and the URL-safe form for use in hrefs.
 */
export async function createPayload(sizeBytes = 64, tier = 'TIER_RAM'): Promise<PayloadIds> {
  const alloc = await apiFetch('/v1/payloads', {
    method: 'POST',
    body: JSON.stringify({ sizeBytes: String(sizeBytes), preferredTier: tier }),
  });
  const raw: string = alloc.payloadDescriptor.payloadId.value;
  const urlSafe = raw.replace(/\+/g, '-').replace(/\//g, '_');

  await apiFetch(`/v1/payloads/${urlSafe}/commit`, {
    method: 'POST',
    body: JSON.stringify({}),
  });

  return { raw, urlSafe };
}

/** Convert standard base64 to URL-safe base64, keeping = padding (grpc-gateway requires it). */
function toURLSafe(id: string): string {
  return id.replace(/\+/g, '-').replace(/\//g, '_');
}

export async function deletePayload(id: string) {
  try {
    await apiFetch(`/v1/payloads/${toURLSafe(id)}?force=true`, { method: 'DELETE' });
  } catch {}
}

export async function createStream(namespace: string, name: string) {
  await apiFetch('/v1/streams', {
    method: 'POST',
    body: JSON.stringify({ stream: { namespace, name } }),
  });
}

export async function deleteStream(namespace: string, name: string) {
  try {
    await apiFetch(`/v1/streams/${namespace}/${name}`, { method: 'DELETE' });
  } catch {}
}

export async function listPayloads(): Promise<any[]> {
  const res = await apiFetch('/v1/payloads');
  return res?.payloads ?? [];
}
