<script>
  let namespace = 'default';
  let name = '';
  let streams = JSON.parse(localStorage.getItem('streams') || '[]');

  function add() {
    if (!name) return;
    streams = [...streams, { namespace, name }];
    localStorage.setItem('streams', JSON.stringify(streams));
    name = '';
  }
</script>

<h2>Streams</h2>
<div>
  <input bind:value={namespace} placeholder="namespace" />
  <input bind:value={name} placeholder="name" />
  <button on:click={add}>Save stream</button>
</div>
<ul>
  {#each streams as s}
    <li>{s.namespace}/{s.name}</li>
  {/each}
</ul>
