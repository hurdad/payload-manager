import { writable } from 'svelte/store';

const state = writable({ stats: null, error: '' });
let timer;

export function statsStore() {
  async function refresh() {
    try {
      const res = await fetch('/v1/admin/stats');
      const stats = await res.json();
      state.set({ stats, error: '' });
    } catch (e) {
      state.set({ stats: null, error: String(e) });
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
