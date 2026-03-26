import { writable } from 'svelte/store';

const state = writable({ loading: false, payloads: [], error: '' });
let timer;

export function payloadStore() {
  async function refresh() {
    state.update((s) => ({ ...s, loading: true, error: '' }));
    try {
      const res = await fetch('/v1/payloads');
      const data = await res.json();
      state.set({ loading: false, payloads: data.payloads || [], error: '' });
    } catch (e) {
      state.set({ loading: false, payloads: [], error: String(e) });
    }
  }

  function start() {
    refresh();
    timer = setInterval(refresh, 5000);
  }

  function stop() {
    if (timer) clearInterval(timer);
  }

  return { subscribe: state.subscribe, refresh, start, stop };
}
