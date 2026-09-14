#!/usr/bin/env python3
"""Emit docs/architecture.svg. Generated rather than hand-placed so the
coordinates stay consistent when a box moves."""

W, H = 1268, 920
P = []          # svg body parts
L = []          # group labels, painted last so edges cannot run through them

def esc(t): return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def group(x, y, w, h, title, cls):
    P.append(f'<rect class="grp {cls}" x="{x}" y="{y}" width="{w}" height="{h}" rx="10"/>')
    # Opaque backing: edges routed into the group would otherwise run straight
    # through the label.
    tw = 8.2 * len(title) + 16
    L.append(f'<rect class="lblbg" x="{x+9}" y="{y+11}" width="{tw}" height="21" rx="4"/>')
    L.append(f'<text class="grpt {cls}t" x="{x+16}" y="{y+26}">{esc(title)}</text>')

# Conservative per-character widths. The browser picks a font from the stack in
# the stylesheet and it will not be the one this script renders with, so the
# estimate is deliberately wider than any of them — a label that fits here fits
# anywhere, which is what stops text spilling out of its box on GitHub.
BOLD_CH, SUB_CH, PAD = 8.4, 6.5, 18
OVERFLOW = []

def check_fit(w, lines, where):
    for i, ln in enumerate(lines):
        est = len(ln) * (BOLD_CH if i == 0 else SUB_CH) + PAD
        if est > w:
            OVERFLOW.append((where, ln, round(est), w))

def box(x, y, w, h, lines, cls="node", anchor_note=None):
    check_fit(w, lines, f"({x},{y})")
    P.append(f'<rect class="{cls}" x="{x}" y="{y}" width="{w}" height="{h}" rx="7"/>')
    n = len(lines)
    # first line bold, rest small
    total = 19 + (n - 1) * 15
    ty = y + (h - total) / 2 + 15
    P.append(f'<text class="lbl" x="{x+w/2}" y="{ty}">{esc(lines[0])}</text>')
    for i, ln in enumerate(lines[1:], 1):
        P.append(f'<text class="sub" x="{x+w/2}" y="{ty + i*15}">{esc(ln)}</text>')
    if anchor_note:
        P.append(f'<text class="note" x="{x+w/2}" y="{y+h+15}">{esc(anchor_note)}</text>')

def cyl(x, y, w, h, label):
    ry = 7
    P.append(f'<path class="store" d="M{x} {y+ry} a{w/2} {ry} 0 0 1 {w} 0 v{h-2*ry} a{w/2} {ry} 0 0 1 {-w} 0 z"/>')
    P.append(f'<path class="storetop" d="M{x} {y+ry} a{w/2} {ry} 0 0 1 {w} 0"/>')
    P.append(f'<text class="lbl" x="{x+w/2}" y="{y+h/2+9}">{esc(label)}</text>')

def arrow(x1, y1, x2, y2, cls="edge", label=None, lx=None, ly=None, dash=False):
    d = ' stroke-dasharray="5 4"' if dash else ''
    P.append(f'<line class="{cls}" x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}"{d} marker-end="url(#a-{cls})"/>')
    if label:
        P.append(f'<text class="elbl" x="{lx if lx is not None else (x1+x2)/2}" y="{ly if ly is not None else (y1+y2)/2-6}">{esc(label)}</text>')

# ---------------------------------------------------------------- clients
group(28, 20, 560, 124, "Clients", "g1")
box(52, 46, 236, 38, ["Browser"])
box(52, 94, 236, 40, ["Svelte web UI", "embedded in the gateway"])
box(316, 40, 248, 50, ["Native client", "C++ / Python"])

# ------------------------------------------------------------ control plane
group(28, 164, 560, 536, "Control plane  —  metadata only", "g2")
box(52, 196, 300, 46, ["gRPC-Gateway", "REST to gRPC  ·  serves the UI  ·  OpenAPI"])
box(52, 266, 512, 46, ["gRPC servers", "admin · catalog · data · ring · stream"])
box(52, 336, 250, 62, ["Service layer", "lifecycle · placement · leasing", "metadata · lineage · streams"])
box(320, 336, 244, 62, ["Ring service", "acquire / commit slots", "lease / release"])
box(52, 424, 250, 46, ["Repository  (internal/db)", "transactions"])
cyl(58,  490, 110, 44, "Memory")
cyl(186, 490, 116, 44, "PostgreSQL")
box(52, 560, 250, 62, ["Placement · Tiering · Spill", "demotes a tier under pressure"])
box(320, 560, 244, 62, ["Observability", "metrics · traces · structured logs",
                        "inert until configured"])

# ------------------------------------------------------------ storage tiers
group(628, 164, 506, 430, "Storage tiers  —  a demotion chain", "g3")
box(668, 204, 236, 50, ["GPU", "CUDA IPC handle"], "tier")
box(668, 306, 236, 50, ["RAM", "POSIX shared memory"], "tier")
box(668, 408, 236, 50, ["Disk", "file"], "tier")
box(668, 510, 236, 50, ["Object storage", "S3 / GCS / Azure"], "tier")
box(940, 408, 170, 50, ["Void", "deleted, not moved"], "void")

for y in (254, 356, 458):
    arrow(786, y, 786, y + 52, "spill", "spill", 812, y + 32)

arrow(904, 229, 940, 418, "dash", "spill_target = TIER_VOID", 1025, 370, dash=True)
arrow(904, 331, 940, 426, "dash", None, dash=True)
arrow(904, 433, 940, 433, "dash", None, dash=True)

# --------------------------------------------------------------- ring tier
group(628, 614, 506, 86, "Ring tier  —  TIER_RAM_RING", "g4")
box(652, 642, 458, 44, ["N pre-allocated /dev/shm slots per ring",
                        "(ring_id, slot_idx, generation)  ·  no PayloadID  ·  no catalog row"], "ring")

# ------------------------------------------------------------- telemetry
# Two lanes out of Alloy so no edge has to cross a box: metrics along the top
# to Prometheus, traces along the bottom to Tempo, both read by Grafana.
group(300, 726, 834, 170, "Telemetry  —  docker-compose.observability.yml", "g5")
box(330, 788, 270, 60, ["Grafana Alloy", "OTLP receiver", "gRPC :4317 · HTTP :4318"], "obs")
box(650, 756, 214, 56, ["Prometheus", "remote_write  ·  :9090"], "obs")
box(650, 828, 214, 56, ["Tempo", "OTLP traces  ·  :3200"], "obs")
box(910, 788, 194, 60, ["Grafana", "dashboards  ·  :3000"], "obs")

arrow(600, 802, 650, 784, "obsedge", "metrics", 622, 776)
arrow(600, 832, 650, 856, "obsedge", "traces", 620, 862)
arrow(864, 784, 910, 802, "obsedge", "query", 888, 776)
arrow(864, 856, 910, 834, "obsedge", "query", 888, 864)

# ------------------------------------------------------------------- edges
arrow(170, 84, 170, 94)                        # browser runs the UI
arrow(170, 134, 170, 196, "edge", "REST / JSON", 216, 170)
arrow(202, 242, 202, 266)                      # gateway -> servers
# The native client never goes through the gateway; that is a browser path.
arrow(440, 84, 440, 266, "edge", "gRPC, direct", 494, 176)
arrow(177, 312, 177, 336)                      # servers -> service layer
arrow(442, 312, 442, 336)                      # servers -> ring service
arrow(177, 398, 177, 424)                      # service -> repository
arrow(140, 470, 118, 490)                      # repo -> memory
arrow(215, 470, 240, 490)                      # repo -> postgres
arrow(177, 534, 177, 560)                      # catalogs column -> tiering
arrow(302, 583, 628, 368, "thin")              # tiering -> tier group
P.append('<path class="thin" d="M442 398 V 657 H 628" marker-end="url(#a-thin)" fill="none"/>')

# data plane
arrow(442, 622, 442, 788, "obsedge", "OTLP  ·  push", 516, 706)
P.append('<path class="data" d="M564 64 H 1215 V 657" fill="none"/>')
P.append('<text class="dlbl" x="880" y="52">data plane  —  bytes move directly, no service in the path</text>')
arrow(1215, 338, 1134, 338, "data")
arrow(1215, 657, 1134, 657, "data")

STYLE = """
  .bg    { fill:#ffffff }
  .lblbg { fill:#ffffff }
  .grp   { fill:none; stroke-width:1.4 }
  .g1    { stroke:#9aa5b1 } .g2 { stroke:#6b8fb5 } .g3 { stroke:#9a8f73 } .g4 { stroke:#5f8f73 } .g5 { stroke:#7d6aa3 }
  .grpt  { font:600 13px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; letter-spacing:.3px }
  .g1t   { fill:#6b7480 } .g2t { fill:#40628a } .g3t { fill:#6f6754 } .g4t { fill:#3f6b53 } .g5t { fill:#5c4a80 }
  .node  { fill:#eef4fb; stroke:#5b7fa6; stroke-width:1.3 }
  .tier  { fill:#f5f1e7; stroke:#9a8f73; stroke-width:1.3 }
  .ring  { fill:#eaf3ee; stroke:#5f8f73; stroke-width:1.3 }
  .void  { fill:#f8eded; stroke:#b07575; stroke-width:1.3 }
  .obs   { fill:#f2eef8; stroke:#7d6aa3; stroke-width:1.3 }
  .store { fill:#e7eef7; stroke:#5b7fa6; stroke-width:1.3 }
  .storetop { fill:none; stroke:#5b7fa6; stroke-width:1.3 }
  .lbl   { font:600 13.5px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; fill:#16202b; text-anchor:middle }
  .sub   { font:11.5px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; fill:#4a5866; text-anchor:middle }
  .note  { font:11px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; fill:#6b7480; text-anchor:middle }
  .elbl  { font:11.5px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; fill:#6f6754; text-anchor:middle }
  .dlbl  { font:600 12px ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; fill:#8a5a2b; text-anchor:middle }
  .edge  { stroke:#5b7fa6; stroke-width:1.6; fill:none }
  .thin  { stroke:#8a94a0; stroke-width:1.4; fill:none }
  .spill { stroke:#9a8f73; stroke-width:2.0; fill:none }
  .dash  { stroke:#b07575; stroke-width:1.5; fill:none }
  .data  { stroke:#c8873f; stroke-width:3.0; fill:none }
  .obsedge { stroke:#7d6aa3; stroke-width:1.6; fill:none }
"""

MARKERS = "".join(
    f'<marker id="a-{n}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
    f'<path d="M0 0 L10 5 L0 10 z" fill="{c}"/></marker>'
    for n, c in [("edge", "#5b7fa6"), ("thin", "#8a94a0"), ("spill", "#9a8f73"),
                 ("dash", "#b07575"), ("data", "#c8873f"), ("obsedge", "#7d6aa3")])

svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
       f'role="img" aria-label="Payload Manager architecture: clients, control plane, storage tier demotion chain, and ring tier">'
       f'<defs>{MARKERS}<style>{STYLE}</style></defs>'
       f'<rect class="bg" x="0" y="0" width="{W}" height="{H}"/>'
       + "".join(P) + "".join(L) + "</svg>")

import pathlib
pathlib.Path("docs/architecture.svg").write_text(svg)
if OVERFLOW:
    print("LABELS THAT MAY OVERFLOW IN A WIDER FONT:")
    for where, ln, est, w in OVERFLOW:
        print(f"  {where}  need ~{est}px, box is {w}px   {ln!r}")
else:
    print("all labels fit with margin")
print("wrote docs/architecture.svg", len(svg), "bytes")
