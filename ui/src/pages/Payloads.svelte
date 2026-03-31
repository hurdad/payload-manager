<script>
  import { onMount, onDestroy } from 'svelte';
  import { fmtBytes } from '../lib/fmt.js';
  import { base64ToUuid } from '../lib/uuid.js';
  import { api, toURLSafe } from '../lib/api.js';

  let payloads = [];
  let loading = true;
  let error = '';
  let tierFilter = localStorage.getItem('pm-tier-filter') ?? '';
  let interval;

  // Pagination
  let pageToken = '';       // current page cursor
  let nextPageToken = '';   // cursor for the next page (empty = last page)
  let pageHistory = [];     // stack of previous cursors for "Prev"
  let totalCount = 0;       // total matching filter

  // Detail panel
  let selectedId = null;
  let detailTab = 'snapshot';
  let snapshot = null;
  let lineage = null;
  let detailError = '';
  let detailLoading = false;

  // Metadata editing
  let metaCache = {};  // id -> { raw, schema, source }
  let metaRaw = '';
  let metaSchema = '';
  let metaSource = '';
  let metaSaving = false;
  let metaMsg = '';

  // Action state
  let actionBusy = {};  // id -> bool
  let actionMsg = {};   // id -> {ok, err}

  // Server clock reference — avoids browser/server clock skew in age display
  let serverNow = null;   // server time (ms) at last fetch
  let fetchedAt = null;   // browser Date.now() at last fetch

  // Sorting — client-side within the current page
  // Default: newest first (desc), matching server ORDER BY created_at_ms DESC
  let sortDir = 'desc'; // 'asc' | 'desc'

  function toggleAgeSort() {
    sortDir = sortDir === 'desc' ? 'asc' : 'desc';
  }

  $: sortedPayloads = sortDir === 'asc'
    ? [...payloads].sort((a, b) => parseInt(a.createdAtMs || '0') - parseInt(b.createdAtMs || '0'))
    : [...payloads].sort((a, b) => parseInt(b.createdAtMs || '0') - parseInt(a.createdAtMs || '0'));

  const TIERS = ['', 'TIER_GPU', 'TIER_RAM', 'TIER_DISK', 'TIER_OBJECT'];
  const TIER_LABELS = { '': 'All', TIER_GPU: 'GPU', TIER_RAM: 'RAM', TIER_DISK: 'Disk', TIER_OBJECT: 'Object' };
  const TIER_CLS   = { TIER_GPU: 'badge-gpu', TIER_RAM: 'badge-ram', TIER_DISK: 'badge-disk', TIER_OBJECT: 'badge-object' };
  // Ordered from fastest to slowest; used to derive spill/promote targets
  const TIER_ORDER = ['TIER_GPU', 'TIER_RAM', 'TIER_DISK', 'TIER_OBJECT'];

  function tierLabel(t) { return TIER_LABELS[t] ?? t?.replace('TIER_','') ?? '—'; }
  function tierClass(t) { return TIER_CLS[t] ?? 'badge-other'; }
  function tiersBelow(t) { const i = TIER_ORDER.indexOf(t); return i < 0 ? [] : TIER_ORDER.slice(i + 1); }
  function tiersAbove(t) { const i = TIER_ORDER.indexOf(t); return i < 0 ? [] : TIER_ORDER.slice(0, i); }

  function stateClass(s) {
    const m = { PAYLOAD_STATE_ACTIVE: 'active', PAYLOAD_STATE_ALLOCATED: 'allocated', PAYLOAD_STATE_DURABLE: 'durable',
                PAYLOAD_STATE_SPILLING: 'spilling', PAYLOAD_STATE_EVICTING: 'evicting', PAYLOAD_STATE_DELETING: 'deleting' };
    return 'badge-' + (m[s] ?? 'other');
  }
  function stateLabel(s) { return s?.replace('PAYLOAD_STATE_','') ?? '—'; }

  function fmtAge(ms) {
    if (!ms || ms === '0') return '—';
    const ts = parseInt(ms);
    if (isNaN(ts)) return '—';
    const now = serverNow != null ? serverNow + (Date.now() - fetchedAt) : Date.now();
    const diff = now - ts;
    if (diff < 0) return '—';
    const s = Math.floor(diff / 1000);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s/60)}m`;
    if (s < 86400) return `${Math.floor(s/3600)}h`;
    return `${Math.floor(s/86400)}d`;
  }

  async function refresh() {
    try {
      error = '';
      const res = await api.listPayloads(tierFilter || undefined, 50, pageToken);
      payloads       = res?.payloads ?? [];
      totalCount     = res?.totalCount ?? 0;
      nextPageToken  = res?.nextPageToken ?? '';
      if (res?.serverNow) { serverNow = res.serverNow; fetchedAt = Date.now(); }
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  function goNextPage() {
    pageHistory = [...pageHistory, pageToken];
    pageToken = nextPageToken;
    refresh();
  }

  function goPrevPage() {
    pageToken = pageHistory[pageHistory.length - 1];
    pageHistory = pageHistory.slice(0, -1);
    refresh();
  }

  function resetPagination() {
    pageToken = '';
    nextPageToken = '';
    pageHistory = [];
  }

  onMount(() => { refresh(); interval = setInterval(refresh, 5000); });
  onDestroy(() => clearInterval(interval));

  async function selectPayload(id) {
    if (selectedId === id) { selectedId = null; return; }
    selectedId = id;
    detailTab = 'snapshot';
    snapshot = null; lineage = null;
    detailError = '';
    await loadDetailTab('snapshot');
  }

  async function loadDetailTab(tab) {
    detailTab = tab;
    detailError = '';
    detailLoading = true;
    try {
      if (tab === 'snapshot') {
        snapshot = await api.snapshot(selectedId);
      } else if (tab === 'lineage') {
        lineage = await api.lineage(selectedId, false, 10);
      } else if (tab === 'metadata') {
        // No GET metadata endpoint — restore cached draft or start empty
        const cached = metaCache[selectedId];
        metaRaw    = cached?.raw    ?? '';
        metaSchema = cached?.schema ?? '';
        metaSource = cached?.source ?? '';
        metaMsg = '';
      }
    } catch (e) {
      detailError = e.message;
    } finally {
      detailLoading = false;
    }
  }

  function updateMetaCache(id) {
    metaCache = { ...metaCache, [id]: { raw: metaRaw, schema: metaSchema, source: metaSource } };
  }

  async function saveMetadata(id) {
    metaSaving = true; metaMsg = '';
    try {
      await api.updateMetadata(id, metaRaw, metaSchema);
      metaMsg = 'Saved';
    } catch (e) {
      metaMsg = 'Error: ' + e.message;
    } finally {
      metaSaving = false;
    }
  }

  async function appendEvent(id) {
    metaSaving = true; metaMsg = '';
    try {
      await api.appendMetadataEvent(id, metaRaw, metaSource);
      metaMsg = 'Event appended';
    } catch (e) {
      metaMsg = 'Error: ' + e.message;
    } finally {
      metaSaving = false;
    }
  }

  function busy(id) { return !!actionBusy[id]; }

  async function runAction(id, fn, label) {
    actionBusy = { ...actionBusy, [id]: true };
    actionMsg = { ...actionMsg, [id]: null };
    try {
      await fn();
      actionMsg = { ...actionMsg, [id]: { ok: label + ' done' } };
      await refresh();
      if (selectedId === id && detailTab === 'snapshot') {
        await loadDetailTab('snapshot');
      }
    } catch (e) {
      actionMsg = { ...actionMsg, [id]: { err: e.message } };
    } finally {
      actionBusy = { ...actionBusy, [id]: false };
    }
  }

  function handleDelete(p) {
    const uuid = base64ToUuid(p.id.value);
    if (!confirm(`Delete payload ${uuid}?\nThis is irreversible.`)) return;
    runAction(p.id.value, () => api.deletePayload(p.id.value, true), 'Delete');
  }

  function handleSpill(p, targetTier) {
    runAction(p.id.value, () => api.spill([p.id.value], targetTier), 'Spill');
  }

  function handlePromote(p, tier) {
    runAction(p.id.value, () => api.promote(p.id.value, tier), 'Promote');
  }

  function handlePin(p) {
    runAction(p.id.value, () => api.pin(p.id.value, 0), 'Pin');
  }

  function handleUnpin(p) {
    runAction(p.id.value, () => api.unpin(p.id.value), 'Unpin');
  }

  function handlePrefetch(p) {
    runAction(p.id.value, () => api.prefetch(p.id.value, 'TIER_RAM'), 'Prefetch');
  }

  function downloadUrl(id) { return `/v1/payloads/${toURLSafe(id)}/download`; }

  function fmtPlacement(snap) {
    if (!snap?.payloadDescriptor) return null;
    const pd = snap.payloadDescriptor;
    if (pd.disk) return { tier: 'DISK', info: `Path: ${pd.disk.path}\nOffset: ${pd.disk.offsetBytes} B\nLength: ${fmtBytes(pd.disk.lengthBytes)}` };
    if (pd.ram)  return { tier: 'RAM',  info: `SHM: ${pd.ram.shmName}\nLength: ${fmtBytes(pd.ram.lengthBytes)}` };
    if (pd.gpu)  return { tier: 'GPU',  info: `Device: ${pd.gpu.deviceId ?? 0}\nLength: ${fmtBytes(pd.gpu.lengthBytes)}` };
    return null;
  }
</script>

<div class="page">
  <div class="page-header">
    <h2>Payloads</h2>
    <div class="header-right">
      <div class="tier-tabs">
        {#each TIERS as t}
          <button
            class="tier-tab"
            class:active={tierFilter === t}
            on:click={() => { tierFilter = t; localStorage.setItem('pm-tier-filter', t); resetPagination(); refresh(); }}
          >{TIER_LABELS[t]}</button>
        {/each}
      </div>
      <button on:click={refresh} title="Refresh">↺</button>
    </div>
  </div>

  {#if error}<p class="error-msg">{error}</p>{/if}

  {#if loading && payloads.length === 0}
    <p class="muted">Loading…</p>
  {:else if payloads.length === 0}
    <p class="muted">No payloads found.</p>
  {:else}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>UUID</th>
            <th>Tier</th>
            <th>State</th>
            <th>Size</th>
            <th class="sortable-th" on:click={toggleAgeSort}>
              Age {sortDir === 'desc' ? '↓' : '↑'}
            </th>
            <th>Leases</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {#each sortedPayloads as p (p.id?.value)}
            {@const uuid = base64ToUuid(p.id?.value ?? '')}
            {@const pinned = p.noEvict || p.pinned}
            <tr
              class="payload-row"
              class:selected={selectedId === p.id?.value}
              on:click={() => selectPayload(p.id?.value)}
            >
              <td class="mono uuid-cell" title={uuid}>{uuid}</td>
              <td><span class="badge {tierClass(p.tier)}">{tierLabel(p.tier)}</span></td>
              <td><span class="badge {stateClass(p.state)}">{stateLabel(p.state)}</span></td>
              <td>{fmtBytes(p.sizeBytes)}</td>
              <td class="muted">{fmtAge(p.createdAtMs)}</td>
              <td class="muted">{p.activeLeaseCount ?? 0}</td>
              <td class="actions-cell" on:click|stopPropagation>
                <!-- Download -->
                <a class="action-btn" href={downloadUrl(p.id?.value)} target="_blank"
                   title={p.tier !== 'TIER_DISK' && p.tier !== 'TIER_OBJECT' ? 'Download (will spill to disk)' : 'Download'}>
                  ↓
                </a>
                <!-- Spill to any tier below -->
                {#each tiersBelow(p.tier) as t}
                  <button class="action-btn" title="Spill to {tierLabel(t)}"
                    disabled={busy(p.id?.value)} on:click={() => handleSpill(p, t)}>⬇{tierLabel(t)}</button>
                {/each}
                <!-- Promote to any tier above -->
                {#each tiersAbove(p.tier) as t}
                  <button class="action-btn" title="Promote to {tierLabel(t)}"
                    disabled={busy(p.id?.value)} on:click={() => handlePromote(p, t)}>⬆{tierLabel(t)}</button>
                {/each}
                <!-- Prefetch -->
                <button class="action-btn" title="Prefetch to RAM"
                  disabled={busy(p.id?.value)} on:click={() => handlePrefetch(p)}>⟳</button>
                <!-- Pin / Unpin -->
                {#if pinned}
                  <button class="action-btn active" title="Unpin"
                    disabled={busy(p.id?.value)} on:click={() => handleUnpin(p)}>📌</button>
                {:else}
                  <button class="action-btn" title="Pin"
                    disabled={busy(p.id?.value)} on:click={() => handlePin(p)}>📌</button>
                {/if}
                <!-- Delete -->
                <button class="action-btn danger" title="Delete"
                  disabled={busy(p.id?.value)} on:click={() => handleDelete(p)}>✕</button>

                {#if actionMsg[p.id?.value]}
                  {#if actionMsg[p.id?.value].ok}
                    <span class="action-status ok">{actionMsg[p.id?.value].ok}</span>
                  {:else}
                    <span class="action-status err">{actionMsg[p.id?.value].err}</span>
                  {/if}
                {/if}
              </td>
            </tr>

            <!-- Detail row -->
            {#if selectedId === p.id?.value}
              <tr class="detail-row">
                <td colspan="7">
                  <div class="detail-panel">
                    <div class="detail-tabs">
                      {#each ['snapshot','lineage','metadata'] as tab}
                        <button
                          class="detail-tab"
                          class:active={detailTab === tab}
                          on:click={() => loadDetailTab(tab)}
                        >{tab}</button>
                      {/each}
                    </div>

                    {#if detailLoading}<p class="muted">Loading…</p>
                    {:else if detailError}<p class="error-msg">{detailError}</p>
                    {:else if detailTab === 'snapshot'}
                      {@const placement = fmtPlacement(snapshot)}
                      <div class="snapshot-content">
                        <div class="kv-group">
                          <div class="kv"><span class="kk">Payload ID</span><span class="kv-val mono">{uuid}</span></div>
                          <div class="kv"><span class="kk">Current Tier</span>
                            <span class="badge {tierClass(snapshot?.payloadDescriptor?.tier)}">{tierLabel(snapshot?.payloadDescriptor?.tier)}</span>
                          </div>
                          <div class="kv"><span class="kk">State</span>
                            <span class="badge {stateClass(p.state)}">{stateLabel(p.state)}</span>
                          </div>
                          <div class="kv"><span class="kk">Size</span><span class="kv-val">{fmtBytes(p.sizeBytes)}</span></div>
                          <div class="kv"><span class="kk">Active Leases</span><span class="kv-val">{p.activeLeaseCount ?? 0}</span></div>
                          <div class="kv"><span class="kk">Pinned</span><span class="kv-val">{pinned ? 'Yes' : 'No'}</span></div>
                          <div class="kv"><span class="kk">Created</span><span class="kv-val">{p.createdAtMs && p.createdAtMs !== '0' ? new Date(parseInt(p.createdAtMs)).toLocaleString() : '—'}</span></div>
                        </div>
                        {#if placement}
                          <div class="placement-box">
                            <div class="form-title">Placement ({placement.tier})</div>
                            <pre class="placement-pre">{placement.info}</pre>
                          </div>
                        {/if}
                      </div>

                    {:else if detailTab === 'lineage'}
                      {#if !lineage?.edges || lineage.edges.length === 0}
                        <p class="muted">No lineage edges.</p>
                      {:else}
                        <table>
                          <thead><tr><th>Parent ID</th><th>Operation</th><th>Role</th></tr></thead>
                          <tbody>
                            {#each lineage.edges as e}
                              <tr>
                                <td class="mono">{e.parentId?.value ? base64ToUuid(e.parentId.value) : '—'}</td>
                                <td>{e.operation ?? '—'}</td>
                                <td>{e.role ?? '—'}</td>
                              </tr>
                            {/each}
                          </tbody>
                        </table>
                      {/if}

                    {:else if detailTab === 'metadata'}
                      <div class="meta-editor">
                        <div class="meta-row">
                          <div class="meta-col">
                            <label class="field-label">Data (raw text / JSON)</label>
                            <textarea bind:value={metaRaw} rows="5" style="width:100%;resize:vertical;font-family:monospace;font-size:12px" placeholder="JSON payload data" on:input={() => updateMetaCache(p.id?.value)}></textarea>
                          </div>
                          <div class="meta-col meta-col-sm">
                            <label class="field-label">Schema (optional)</label>
                            <input bind:value={metaSchema} placeholder="schema" style="width:100%" />
                            <label class="field-label" style="margin-top:0.5rem">Source (for event)</label>
                            <input bind:value={metaSource} placeholder="source" style="width:100%" />
                          </div>
                        </div>
                        <div class="meta-actions">
                          <button class="btn-primary" disabled={metaSaving} on:click={() => saveMetadata(p.id?.value)}>Update Metadata</button>
                          <button disabled={metaSaving} on:click={() => appendEvent(p.id?.value)}>Append Event</button>
                          {#if metaMsg}<span class="{metaMsg.startsWith('Error') ? 'error-msg' : 'success-msg'}" style="margin-left:0.5rem">{metaMsg}</span>{/if}
                        </div>
                      </div>
                    {/if}
                  </div>
                </td>
              </tr>
            {/if}
          {/each}
        </tbody>
      </table>
    </div>

    <div class="pagination-bar">
      <p class="muted count-line">
        {#if totalCount > 0}
          {pageHistory.length * 50 + 1}–{pageHistory.length * 50 + payloads.length} of {totalCount} payload{totalCount === 1 ? '' : 's'}
        {:else}
          0 payloads
        {/if}
      </p>
      <div class="page-btns">
        <button class="page-btn" disabled={pageHistory.length === 0} on:click={goPrevPage}>← Prev</button>
        <button class="page-btn" disabled={!nextPageToken} on:click={goNextPage}>Next →</button>
      </div>
    </div>
  {/if}
</div>

<style>
  .page { max-width: 100%; }

  .page-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1rem;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .page-header h2 { margin: 0; font-size: 16px; font-weight: 700; }

  .header-right { display: flex; align-items: center; gap: 0.5rem; }

  .tier-tabs { display: flex; gap: 2px; }
  .tier-tab {
    padding: 0.25rem 0.6rem;
    font-size: 12px;
    border-radius: 4px;
    background: transparent;
    border: 1px solid var(--buttonBorder);
    color: var(--muted);
    cursor: pointer;
    transition: all 0.1s;
  }
  .tier-tab:hover { color: var(--text); background: var(--button); }
  .tier-tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }

  .table-wrap {
    overflow-x: auto;
    border-radius: 8px;
    border: 1px solid var(--border);
  }

  .sortable-th {
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  .sortable-th:hover { color: var(--accent); }

  .payload-row { cursor: pointer; }
  .payload-row.selected td { background: color-mix(in srgb, var(--accent) 6%, transparent) !important; }

  .uuid-cell {
    font-size: 11.5px;
    max-width: 220px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .mono { font-family: 'JetBrains Mono', 'Fira Code', ui-monospace, monospace; }

  .actions-cell {
    display: flex;
    align-items: center;
    gap: 2px;
    white-space: nowrap;
  }

  .action-btn {
    background: transparent;
    border: 1px solid transparent;
    padding: 0.1rem 0.35rem;
    font-size: 13px;
    border-radius: 4px;
    cursor: pointer;
    color: var(--muted);
    transition: all 0.1s;
    text-decoration: none;
    display: inline-flex;
    align-items: center;
  }
  .action-btn:hover { background: var(--button); border-color: var(--buttonBorder); color: var(--text); }
  .action-btn:disabled { opacity: 0.3; cursor: default; }
  .action-btn.danger:hover { color: var(--danger); border-color: var(--danger); background: color-mix(in srgb, var(--danger) 10%, transparent); }
  .action-btn.active { color: var(--accent); }

  .action-status { font-size: 11px; padding-left: 0.25rem; }
  .action-status.ok { color: var(--success); }
  .action-status.err { color: var(--danger); }

  .detail-row td { padding: 0; background: color-mix(in srgb, var(--accent) 3%, transparent) !important; }
  .detail-panel {
    padding: 1rem 1.25rem;
    border-top: 1px solid var(--border);
  }

  .detail-tabs {
    display: flex;
    gap: 4px;
    margin-bottom: 0.75rem;
  }
  .detail-tab {
    padding: 0.25rem 0.7rem;
    font-size: 12px;
    border-radius: 4px;
    background: transparent;
    border: 1px solid var(--buttonBorder);
    color: var(--muted);
    cursor: pointer;
    text-transform: capitalize;
  }
  .detail-tab.active { background: var(--accent); color: #fff; border-color: var(--accent); }

  .snapshot-content { display: flex; gap: 1.5rem; flex-wrap: wrap; }

  .kv-group { display: flex; flex-direction: column; gap: 0.35rem; min-width: 240px; }
  .kv { display: flex; align-items: center; gap: 0.5rem; font-size: 12.5px; }
  .kk { color: var(--muted); font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; min-width: 110px; }
  .kv-val { font-weight: 500; }

  .placement-box { min-width: 220px; }
  .form-title { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin-bottom: 0.4rem; }
  .placement-pre {
    background: var(--button);
    border: 1px solid var(--buttonBorder);
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    font-size: 12px;
    font-family: ui-monospace, monospace;
    margin: 0;
    white-space: pre-wrap;
    word-break: break-all;
  }

  .meta-editor { display: flex; flex-direction: column; gap: 0.75rem; max-width: 700px; }
  .meta-row { display: flex; gap: 1rem; }
  .meta-col { flex: 1; display: flex; flex-direction: column; }
  .meta-col-sm { flex: 0 0 180px; }
  .field-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); margin-bottom: 0.3rem; }
  .meta-actions { display: flex; align-items: center; gap: 0.4rem; }

  .pagination-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-top: 0.5rem;
    gap: 0.5rem;
  }
  .count-line { font-size: 12px; margin: 0; }
  .page-btns { display: flex; gap: 4px; }
  .page-btn {
    padding: 0.2rem 0.6rem;
    font-size: 12px;
    border-radius: 4px;
    background: transparent;
    border: 1px solid var(--buttonBorder);
    color: var(--muted);
    cursor: pointer;
  }
  .page-btn:hover:not(:disabled) { color: var(--text); background: var(--button); }
  .page-btn:disabled { opacity: 0.3; cursor: default; }
</style>
