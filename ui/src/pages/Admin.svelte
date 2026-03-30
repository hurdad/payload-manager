<script>
  import { onMount, onDestroy } from 'svelte';
  import { fmtBytes } from '../lib/fmt.js';
  import { api } from '../lib/api.js';

  let stats = null;
  let error = '';
  let interval;

  async function refresh() {
    try {
      error = '';
      stats = await api.stats();
    } catch (e) {
      error = e.message;
    }
  }

  onMount(() => { refresh(); interval = setInterval(refresh, 5000); });
  onDestroy(() => clearInterval(interval));

  const tiers = [
    { key: 'gpu',    label: 'GPU',    cls: 'badge-gpu',    icon: '◈' },
    { key: 'ram',    label: 'RAM',    cls: 'badge-ram',    icon: '▦' },
    { key: 'disk',   label: 'Disk',   cls: 'badge-disk',   icon: '⬡' },
    { key: 'object', label: 'Object', cls: 'badge-object', icon: '☁' },
  ];

  function count(t) { return stats?.[`payloads${t.charAt(0).toUpperCase()+t.slice(1)}`] ?? '0'; }
  function bytes(t) { return stats?.[`bytes${t.charAt(0).toUpperCase()+t.slice(1)}`] ?? '0'; }
  function total(field) {
    return tiers.reduce((sum, t) => {
      const v = parseInt(stats?.[`${field}${t.key.charAt(0).toUpperCase()+t.key.slice(1)}`] ?? '0');
      return sum + (isNaN(v) ? 0 : v);
    }, 0);
  }
</script>

<div class="page">
  <div class="page-header">
    <h2>Admin</h2>
    <button on:click={refresh} title="Refresh">↺ Refresh</button>
  </div>

  {#if error}<p class="error-msg">{error}</p>{/if}

  {#if stats}
    <div class="tier-grid">
      {#each tiers as t}
        <div class="tier-card">
          <div class="tier-header">
            <span class="tier-icon">{t.icon}</span>
            <span class="badge {t.cls}">{t.label}</span>
          </div>
          <div class="tier-stat">
            <span class="stat-value">{count(t.key)}</span>
            <span class="stat-label">payloads</span>
          </div>
          <div class="tier-stat secondary">
            <span class="stat-value">{fmtBytes(bytes(t.key))}</span>
            <span class="stat-label">used</span>
          </div>
        </div>
      {/each}
    </div>

    <div class="totals card" style="margin-top: 1rem;">
      <div class="totals-row">
        <span class="muted">Total payloads</span>
        <strong>{total('payloads')}</strong>
      </div>
      <div class="totals-row">
        <span class="muted">Total bytes</span>
        <strong>{fmtBytes(total('bytes'))}</strong>
      </div>
    </div>
  {:else if !error}
    <p class="muted">Loading…</p>
  {/if}
</div>

<style>
  .page { max-width: 640px; }

  .page-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1.25rem;
  }
  .page-header h2 {
    margin: 0;
    font-size: 16px;
    font-weight: 700;
  }

  .tier-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 0.75rem;
  }

  .tier-card {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
  }

  .tier-header {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }
  .tier-icon {
    font-size: 16px;
    color: var(--muted);
  }

  .tier-stat {
    display: flex;
    flex-direction: column;
    margin-bottom: 0.25rem;
  }
  .tier-stat.secondary { margin-top: 0.25rem; }

  .stat-value {
    font-size: 20px;
    font-weight: 700;
    line-height: 1.2;
  }
  .tier-stat.secondary .stat-value {
    font-size: 14px;
    font-weight: 600;
    color: var(--muted);
  }

  .stat-label {
    font-size: 11px;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .totals { max-width: 300px; }
  .totals-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.35rem 0;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
  }
  .totals-row:last-child { border-bottom: none; }
</style>
