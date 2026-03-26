<script>
  import { onDestroy, onMount } from 'svelte';
  import { statsStore } from '../stores/stats';
  import { fmtBytes } from '../lib/fmt';

  const store = statsStore();
  let data;
  const unsub = store.subscribe((v) => (data = v));

  onMount(() => store.start());
  onDestroy(() => {
    store.stop();
    unsub();
  });
</script>

<h2>Admin stats</h2>
{#if data?.stats}
  <ul>
    <li>GPU: {data.stats.payloadsGpu} payloads / {fmtBytes(data.stats.bytesGpu)}</li>
    <li>RAM: {data.stats.payloadsRam} payloads / {fmtBytes(data.stats.bytesRam)}</li>
    <li>Disk: {data.stats.payloadsDisk} payloads / {fmtBytes(data.stats.bytesDisk)}</li>
    <li>Object: {data.stats.payloadsObject} payloads / {fmtBytes(data.stats.bytesObject)}</li>
  </ul>
{:else}
  <p>{data?.error || 'Loading…'}</p>
{/if}
