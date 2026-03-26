export function uuidToBase64(uuid) {
  const hex = uuid.replace(/-/g, '');
  const bytes = new Uint8Array(hex.match(/.{1,2}/g).map((b) => parseInt(b, 16)));
  let raw = '';
  bytes.forEach((b) => (raw += String.fromCharCode(b)));
  return btoa(raw).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

export function base64ToUuid(input) {
  const b64 = input.replace(/-/g, '+').replace(/_/g, '/');
  const raw = atob(b64.padEnd(Math.ceil(b64.length / 4) * 4, '='));
  const hex = Array.from(raw)
    .map((c) => c.charCodeAt(0).toString(16).padStart(2, '0'))
    .join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
