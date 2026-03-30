<script>
  import { onMount } from 'svelte';
  import Payloads from './pages/Payloads.svelte';
  import Streams from './pages/Streams.svelte';
  import Admin from './pages/Admin.svelte';

  let page = 'payloads';
  let sidebarOpen = true;
  let theme = 'dark';

  const themes = {
    dark:  { bg: '#0b1220', panel: '#0f172a', border: '#1e293b', text: '#e2e8f0', muted: '#64748b', accent: '#3b82f6', accentText: '#fff', button: '#1e293b', buttonBorder: '#334155', danger: '#ef4444', success: '#22c55e', warn: '#f59e0b' },
    light: { bg: '#f1f5f9', panel: '#ffffff',  border: '#e2e8f0', text: '#0f172a', muted: '#64748b', accent: '#2563eb', accentText: '#fff', button: '#e2e8f0', buttonBorder: '#cbd5e1', danger: '#dc2626', success: '#16a34a', warn: '#d97706' },
  };

  function applyTheme(t) {
    theme = t;
    localStorage.setItem('pm-ui-theme', t);
    const root = document.documentElement;
    Object.entries(themes[t]).forEach(([k, v]) => root.style.setProperty(`--${k}`, v));
  }

  function toggleTheme() { applyTheme(theme === 'dark' ? 'light' : 'dark'); }

  onMount(() => {
    const saved = localStorage.getItem('pm-ui-theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    applyTheme(saved || (prefersDark ? 'dark' : 'light'));
    const savedSidebar = localStorage.getItem('pm-ui-sidebar');
    if (savedSidebar !== null) sidebarOpen = savedSidebar === 'true';
  });

  function toggleSidebar() {
    sidebarOpen = !sidebarOpen;
    localStorage.setItem('pm-ui-sidebar', String(sidebarOpen));
  }

  function nav(p) { page = p; }

  const navItems = [
    { id: 'payloads', label: 'Payloads',  icon: '▦' },
    { id: 'streams',  label: 'Streams',   icon: '⇌' },
    { id: 'admin',    label: 'Admin',     icon: '◈' },
  ];
</script>

<div class="app">
  <header>
    <div class="header-left">
      <span class="logo">⬡</span>
      <span class="title">Payload Manager</span>
    </div>
    <div class="header-right">
      <button class="icon-btn" title="Toggle theme" on:click={toggleTheme}>
        {theme === 'dark' ? '○' : '●'}
      </button>
      <button class="icon-btn" title="Toggle navigation" on:click={toggleSidebar}>
        {sidebarOpen ? '›' : '‹'}
      </button>
    </div>
  </header>

  <div class="body">
    <main>
      {#if page === 'payloads'}
        <Payloads />
      {:else if page === 'streams'}
        <Streams />
      {:else}
        <Admin />
      {/if}
    </main>

    <aside class:open={sidebarOpen}>
      <nav>
        {#each navItems as item}
          <button
            class="nav-item"
            class:active={page === item.id}
            on:click={() => nav(item.id)}
          >
            <span class="nav-icon">{item.icon}</span>
            <span class="nav-label">{item.label}</span>
          </button>
        {/each}
      </nav>
    </aside>
  </div>
</div>

<style>
  :root {
    --sidebar-width: 160px;
    --header-height: 48px;
  }

  :global(*, *::before, *::after) { box-sizing: border-box; }

  :global(body) {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: 'Inter', system-ui, -apple-system, sans-serif;
    font-size: 13px;
    line-height: 1.5;
  }

  :global(input), :global(textarea), :global(select) {
    background: var(--button);
    color: var(--text);
    border: 1px solid var(--buttonBorder);
    border-radius: 4px;
    padding: 0.3rem 0.5rem;
    font-size: 13px;
    font-family: inherit;
    outline: none;
  }
  :global(input:focus), :global(textarea:focus), :global(select:focus) {
    border-color: var(--accent);
    box-shadow: 0 0 0 2px color-mix(in srgb, var(--accent) 25%, transparent);
  }

  :global(table) {
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
  }
  :global(th) {
    background: var(--panel);
    color: var(--muted);
    font-weight: 600;
    font-size: 11px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 0.5rem 0.75rem;
    border-bottom: 1px solid var(--border);
    text-align: left;
    white-space: nowrap;
  }
  :global(td) {
    padding: 0.45rem 0.75rem;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
  }
  :global(tr:hover td) { background: color-mix(in srgb, var(--accent) 5%, transparent); }

  :global(button) {
    background: var(--button);
    color: var(--text);
    border: 1px solid var(--buttonBorder);
    border-radius: 4px;
    padding: 0.3rem 0.65rem;
    font-size: 12.5px;
    font-family: inherit;
    cursor: pointer;
    transition: background 0.1s, border-color 0.1s;
    white-space: nowrap;
  }
  :global(button:hover) {
    background: color-mix(in srgb, var(--button) 70%, var(--accent) 30%);
    border-color: var(--accent);
  }
  :global(button:disabled) { opacity: 0.45; cursor: default; }

  :global(.btn-primary) {
    background: var(--accent);
    color: var(--accentText);
    border-color: var(--accent);
  }
  :global(.btn-primary:hover) {
    background: color-mix(in srgb, var(--accent) 85%, black 15%);
  }
  :global(.btn-danger) {
    background: transparent;
    color: var(--danger);
    border-color: var(--danger);
  }
  :global(.btn-danger:hover) {
    background: color-mix(in srgb, var(--danger) 15%, transparent);
  }

  :global(.badge) {
    display: inline-block;
    padding: 0.1em 0.45em;
    border-radius: 3px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.03em;
  }
  :global(.badge-gpu)    { background: color-mix(in srgb, #a855f7 20%, transparent); color: #c084fc; border: 1px solid #a855f720; }
  :global(.badge-ram)    { background: color-mix(in srgb, #3b82f6 20%, transparent); color: #60a5fa; border: 1px solid #3b82f620; }
  :global(.badge-disk)   { background: color-mix(in srgb, #22c55e 20%, transparent); color: #4ade80; border: 1px solid #22c55e20; }
  :global(.badge-object) { background: color-mix(in srgb, #f59e0b 20%, transparent); color: #fbbf24; border: 1px solid #f59e0b20; }
  :global(.badge-active)    { background: color-mix(in srgb, #22c55e 20%, transparent); color: #4ade80; border: 1px solid #22c55e20; }
  :global(.badge-allocated) { background: color-mix(in srgb, #f59e0b 20%, transparent); color: #fbbf24; border: 1px solid #f59e0b20; }
  :global(.badge-durable)   { background: color-mix(in srgb, #06b6d4 20%, transparent); color: #22d3ee; border: 1px solid #06b6d420; }
  :global(.badge-spilling)  { background: color-mix(in srgb, #f97316 20%, transparent); color: #fb923c; border: 1px solid #f9731620; }
  :global(.badge-evicting)  { background: color-mix(in srgb, #f97316 20%, transparent); color: #fb923c; border: 1px solid #f9731620; }
  :global(.badge-deleting)  { background: color-mix(in srgb, #ef4444 20%, transparent); color: #f87171; border: 1px solid #ef444420; }
  :global(.badge-other)     { background: color-mix(in srgb, #64748b 20%, transparent); color: #94a3b8; border: 1px solid #64748b20; }

  :global(.card) {
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.25rem;
  }

  :global(.error-msg) { color: var(--danger); font-size: 12px; margin: 0.5rem 0; }
  :global(.success-msg) { color: var(--success); font-size: 12px; margin: 0.5rem 0; }
  :global(.muted) { color: var(--muted); }

  .app {
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
  }

  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: var(--header-height);
    padding: 0 1rem;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    z-index: 10;
  }

  .header-left {
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .logo {
    font-size: 18px;
    color: var(--accent);
  }

  .title {
    font-size: 14px;
    font-weight: 700;
    letter-spacing: -0.01em;
    color: var(--text);
  }

  .header-right {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  .icon-btn {
    background: transparent;
    border: 1px solid transparent;
    padding: 0.2rem 0.5rem;
    font-size: 14px;
    color: var(--muted);
    border-radius: 4px;
    cursor: pointer;
    transition: color 0.1s, background 0.1s;
  }
  .icon-btn:hover {
    color: var(--text);
    background: var(--button);
    border-color: var(--buttonBorder);
  }

  .body {
    display: flex;
    flex: 1;
    overflow: hidden;
  }

  main {
    flex: 1;
    overflow-y: auto;
    padding: 1.25rem;
    min-width: 0;
  }

  aside {
    width: 0;
    overflow: hidden;
    background: var(--panel);
    border-right: 1px solid var(--border);
    transition: width 0.2s ease;
    flex-shrink: 0;
    order: -1;
  }
  aside.open {
    width: var(--sidebar-width);
  }

  nav {
    display: flex;
    flex-direction: column;
    padding: 0.75rem 0.5rem;
    gap: 0.25rem;
    width: var(--sidebar-width);
  }

  .nav-item {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.5rem 0.75rem;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    text-align: left;
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    cursor: pointer;
    transition: all 0.1s;
    white-space: nowrap;
    width: 100%;
  }
  .nav-item:hover {
    color: var(--text);
    background: var(--button);
    border-color: var(--buttonBorder);
  }
  .nav-item.active {
    color: var(--accent);
    background: color-mix(in srgb, var(--accent) 10%, transparent);
    border-color: color-mix(in srgb, var(--accent) 30%, transparent);
  }

  .nav-icon {
    font-size: 14px;
    width: 16px;
    text-align: center;
  }
</style>
