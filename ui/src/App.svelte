<script>
  import { onMount } from 'svelte';
  import Payloads from './pages/Payloads.svelte';
  import Streams from './pages/Streams.svelte';
  import Admin from './pages/Admin.svelte';

  let page = 'payloads';
  let theme = 'dark';

  const themes = {
    dark: {
      bg: '#0b1220',
      panel: '#0f172a',
      text: '#e2e8f0',
      muted: '#94a3b8',
      button: '#334155',
      buttonBorder: '#475569'
    },
    light: {
      bg: '#f8fafc',
      panel: '#e2e8f0',
      text: '#0f172a',
      muted: '#334155',
      button: '#cbd5e1',
      buttonBorder: '#94a3b8'
    }
  };

  function applyTheme(nextTheme) {
    theme = nextTheme;
    localStorage.setItem('pm-ui-theme', nextTheme);
    const root = document.documentElement;
    const palette = themes[nextTheme];
    Object.entries(palette).forEach(([name, value]) => {
      root.style.setProperty(`--${name}`, value);
    });
  }

  function toggleTheme() {
    applyTheme(theme === 'dark' ? 'light' : 'dark');
  }

  onMount(() => {
    const saved = localStorage.getItem('pm-ui-theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    applyTheme(saved || (prefersDark ? 'dark' : 'light'));
  });
</script>

<nav>
  <div class="tabs">
    <button on:click={() => (page = 'payloads')}>Payloads</button>
    <button on:click={() => (page = 'streams')}>Streams</button>
    <button on:click={() => (page = 'admin')}>Admin</button>
  </div>
  <button on:click={toggleTheme}>{theme === 'dark' ? '☀️ Light' : '🌙 Dark'}</button>
</nav>

<main>
  {#if page === 'payloads'}
    <Payloads />
  {:else if page === 'streams'}
    <Streams />
  {:else}
    <Admin />
  {/if}
</main>

<style>
  :global(body) {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: system-ui, sans-serif;
  }

  :global(input),
  :global(textarea),
  :global(select) {
    background: color-mix(in srgb, var(--bg) 80%, white 20%);
    color: var(--text);
    border: 1px solid var(--buttonBorder);
    border-radius: 0.25rem;
    padding: 0.3rem 0.5rem;
  }

  :global(table) {
    width: 100%;
    border-collapse: collapse;
    margin-top: 0.8rem;
  }

  :global(th),
  :global(td) {
    border-bottom: 1px solid var(--buttonBorder);
    text-align: left;
    padding: 0.35rem 0.4rem;
  }

  nav {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 1rem;
    background: var(--panel);
    border-bottom: 1px solid var(--buttonBorder);
  }

  .tabs {
    display: flex;
    gap: 0.5rem;
  }

  button {
    color: var(--text);
    background: var(--button);
    border: 1px solid var(--buttonBorder);
    padding: 0.4rem 0.75rem;
    border-radius: 0.25rem;
    cursor: pointer;
  }

  main {
    padding: 1rem;
  }
</style>
