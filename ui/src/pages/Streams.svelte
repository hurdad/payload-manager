<script>
  import { api } from '../lib/api.js';
  import { base64ToUuid } from '../lib/uuid.js';

  // Persisted stream list
  let streams = JSON.parse(localStorage.getItem('pm-streams') || '[]');
  // e.g. [{ namespace: 'default', name: 'my-stream' }]

  let newNs = 'default';
  let newName = '';
  let createError = '';
  let createOk = '';

  // Selected stream for viewing
  let selected = null;
  let entries = null;
  let entriesLoading = false;
  let entriesError = '';
  let consumerError = '';
  let startOffset = 0;
  let maxEntries = 50;
  let consumerGroup = 'ui';
  let committedOffset = null;

  // Append
  let appendPayloadId = '';
  let appendError = '';
  let appendOk = '';

  function saveStreams() { localStorage.setItem('pm-streams', JSON.stringify(streams)); }

  async function createStream() {
    createError = ''; createOk = '';
    try {
      await api.createStream(newNs, newName);
      if (!streams.find(s => s.namespace === newNs && s.name === newName)) {
        streams = [...streams, { namespace: newNs, name: newName }];
        saveStreams();
      }
      createOk = `Stream ${newNs}/${newName} created`;
      newName = '';
    } catch (e) { createError = e.message; }
  }

  async function deleteStream(s) {
    if (!confirm(`Delete stream ${s.namespace}/${s.name}?`)) return;
    try {
      await api.deleteStream(s.namespace, s.name);
      streams = streams.filter(x => !(x.namespace === s.namespace && x.name === s.name));
      saveStreams();
      if (selected === s) selected = null;
    } catch (e) { alert(e.message); }
  }

  async function selectStream(s) {
    selected = s;
    entries = null;
    entriesError = '';
    consumerError = '';
    committedOffset = null;
    await loadEntries();
  }

  async function loadEntries() {
    if (!selected) return;
    entriesLoading = true;
    entriesError = '';
    consumerError = '';
    try {
      const res = await api.readStream(selected.namespace, selected.name, startOffset, maxEntries);
      entries = res?.entries || [];
    } catch (e) {
      entriesError = e.message;
      entries = [];
    } finally {
      entriesLoading = false;
    }
    try {
      const r = await api.getCommitted(selected.namespace, selected.name, consumerGroup);
      committedOffset = r?.offset ?? null;
    } catch (e) {
      consumerError = e.message;
    }
  }

  async function appendEntry() {
    appendError = ''; appendOk = '';
    if (!appendPayloadId.trim()) { appendError = 'Payload ID required'; return; }
    try {
      await api.appendStream(selected.namespace, selected.name, [appendPayloadId.trim()]);
      appendOk = 'Appended';
      appendPayloadId = '';
      await loadEntries();
    } catch (e) { appendError = e.message; }
  }

  async function commitOffset() {
    if (entries === null || entries.length === 0) return;
    const last = entries[entries.length - 1];
    try {
      await api.commitOffset(selected.namespace, selected.name, consumerGroup, last.offset);
      committedOffset = last.offset;
    } catch (e) { alert(e.message); }
  }

  function addLocalOnly() {
    if (!newNs || !newName) return;
    if (!streams.find(s => s.namespace === newNs && s.name === newName)) {
      streams = [...streams, { namespace: newNs, name: newName }];
      saveStreams();
    }
    createOk = 'Added to local list';
    newName = '';
  }
</script>

<div class="page">
  <div class="page-header">
    <h2>Streams</h2>
  </div>

  <div class="layout">
    <!-- Left: stream list + create -->
    <div class="stream-sidebar">
      <div class="card create-card">
        <div class="form-title">New Stream</div>
        <input bind:value={newNs} placeholder="Namespace" style="width:100%; margin-bottom:0.4rem;" />
        <input bind:value={newName} placeholder="Name" style="width:100%; margin-bottom:0.6rem;"
          on:keydown={(e) => e.key === 'Enter' && createStream()} />
        <div class="btn-row">
          <button class="btn-primary" on:click={createStream} disabled={!newNs || !newName}>Create</button>
          <button on:click={addLocalOnly} disabled={!newNs || !newName} title="Track locally without creating">+ Local</button>
        </div>
        {#if createError}<p class="error-msg">{createError}</p>{/if}
        {#if createOk}<p class="success-msg">{createOk}</p>{/if}
      </div>

      <div class="stream-list">
        {#if streams.length === 0}
          <p class="muted empty-hint">No streams. Create one above.</p>
        {/if}
        {#each streams as s}
          <div class="stream-item" class:active={selected === s} role="button" tabindex="0"
            on:click={() => selectStream(s)} on:keydown={(e) => e.key === 'Enter' && selectStream(s)}>
            <div class="stream-name">{s.namespace}/<strong>{s.name}</strong></div>
            <button class="btn-danger icon-action" on:click|stopPropagation={() => deleteStream(s)} title="Delete stream">×</button>
          </div>
        {/each}
      </div>
    </div>

    <!-- Right: stream viewer -->
    <div class="stream-content">
      {#if !selected}
        <div class="empty-state">Select a stream to view entries</div>
      {:else}
        <div class="viewer-header">
          <span class="stream-path">{selected.namespace}/{selected.name}</span>
          <div class="viewer-controls">
            <input type="number" bind:value={startOffset} min="0" style="width:80px" />
            <select bind:value={maxEntries}>
              <option value={20}>20</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
            <button on:click={loadEntries}>↺ Load</button>
          </div>
        </div>

        {#if entriesError}<p class="error-msg">{entriesError}</p>{/if}

        {#if entriesLoading}
          <p class="muted">Loading…</p>
        {:else if entries !== null}
          {#if entries.length === 0}
            <p class="muted" style="margin: 1rem 0">No entries in range.</p>
          {:else}
            <div class="table-wrap">
              <table>
                <thead>
                  <tr><th>Offset</th><th>Payload ID</th><th>Event Time</th></tr>
                </thead>
                <tbody>
                  {#each entries as e}
                    <tr>
                      <td class="mono">{e.offset}</td>
                      <td class="mono">{e.payloadId?.value ? base64ToUuid(e.payloadId.value) : '—'}</td>
                      <td class="muted">{e.eventTime ? new Date(e.eventTime).toLocaleString() : '—'}</td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}

          <!-- Consumer group / commit -->
          <div class="consumer-row">
            <input bind:value={consumerGroup} placeholder="Consumer group" style="width:140px" />
            <button on:click={commitOffset} disabled={!entries || entries.length === 0}>Commit last offset</button>
            {#if committedOffset !== null}
              <span class="muted">Committed: {committedOffset}</span>
            {/if}
            {#if consumerError}<span class="error-msg">{consumerError}</span>{/if}
          </div>
        {/if}

        <!-- Append -->
        <div class="append-section card">
          <div class="form-title">Append Entry</div>
          <div class="append-row">
            <input bind:value={appendPayloadId} placeholder="Payload ID (base64 or UUID)" style="flex:1"
              on:keydown={(e) => e.key === 'Enter' && appendEntry()} />
            <button class="btn-primary" on:click={appendEntry}>Append</button>
          </div>
          {#if appendError}<p class="error-msg">{appendError}</p>{/if}
          {#if appendOk}<p class="success-msg">{appendOk}</p>{/if}
        </div>
      {/if}
    </div>
  </div>
</div>

<style>
  .page { height: 100%; display: flex; flex-direction: column; }

  .page-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 1rem;
    flex-shrink: 0;
  }
  .page-header h2 { margin: 0; font-size: 16px; font-weight: 700; }

  .layout {
    display: flex;
    gap: 1rem;
    flex: 1;
    min-height: 0;
  }

  .stream-sidebar {
    width: 220px;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    overflow-y: auto;
  }

  .create-card { padding: 0.75rem; }
  .form-title { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin-bottom: 0.5rem; }
  .btn-row { display: flex; gap: 0.4rem; }

  .stream-list { display: flex; flex-direction: column; gap: 2px; }
  .stream-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.45rem 0.65rem;
    border-radius: 6px;
    cursor: pointer;
    border: 1px solid transparent;
    transition: all 0.1s;
  }
  .stream-item:hover { background: var(--button); border-color: var(--buttonBorder); }
  .stream-item.active { background: color-mix(in srgb, var(--accent) 10%, transparent); border-color: color-mix(in srgb, var(--accent) 30%, transparent); }
  .stream-name { font-size: 12.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

  .icon-action { padding: 0.1rem 0.4rem; font-size: 14px; line-height: 1; opacity: 0.6; }
  .icon-action:hover { opacity: 1; }

  .stream-content {
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    min-width: 0;
    overflow-y: auto;
  }

  .empty-state {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 200px;
    color: var(--muted);
    border: 1px dashed var(--border);
    border-radius: 8px;
    font-size: 13px;
  }

  .viewer-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.75rem;
    flex-wrap: wrap;
  }
  .stream-path { font-weight: 600; font-size: 13px; }
  .viewer-controls { display: flex; align-items: center; gap: 0.4rem; }

  .table-wrap { overflow-x: auto; border-radius: 6px; border: 1px solid var(--border); }
  .mono { font-family: 'JetBrains Mono', 'Fira Code', ui-monospace, monospace; font-size: 12px; }

  .consumer-row {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 12.5px;
  }

  .append-section { padding: 0.75rem; }
  .append-row { display: flex; gap: 0.4rem; align-items: center; }
  .empty-hint { font-size: 12px; text-align: center; margin-top: 0.5rem; }
</style>
