<script>
  import { onMount, onDestroy } from 'svelte';
  import { payloadStore } from '../stores/payloads';
  import { fmtBytes } from '../lib/fmt';

  const store = payloadStore();
  let data;
  const unsub = store.subscribe((v) => (data = v));

  onMount(() => store.start());
  onDestroy(() => {
    store.stop();
    unsub();
  });

  function downloadHref(payloadId) {
    if (!payloadId) return '#';
    return `/v1/payloads/${encodeURIComponent(payloadId)}/download`;
  }
</script>

<h2>Payloads</h2>
<button on:click={store.refresh}>Refresh</button>

{#if data?.error}<p>{data.error}</p>{/if}
<table>
  <thead><tr><th>ID</th><th>Tier</th><th>State</th><th>Size</th><th>Leases</th><th>Actions</th></tr></thead>
  <tbody>
    {#each data?.payloads || [] as p}
      <tr>
        <td>{p.id?.value}</td>
        <td>{p.tier}</td>
        <td>{p.state}</td>
        <td>{fmtBytes(p.sizeBytes)}</td>
        <td>{p.activeLeases}</td>
        <td><a href={downloadHref(p.id?.value)}>Download</a></td>
      </tr>
    {/each}
  </tbody>
</table>
