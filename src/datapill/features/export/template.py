PROFILE_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>datapill · profile {run_id}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root {{
  --bg:         #f5f7fa;
  --surface:    #ffffff;
  --surface2:   #f0f2f7;
  --border:     #dde1ea;
  --text:       #1a1f2e;
  --text-muted: #6b7280;
  --accent:     #4f46e5;
  --accent2:    #0891b2;
  --green:      #059669;
  --yellow:     #d97706;
  --red:        #dc2626;
  --magenta:    #9333ea;
  --radius:     10px;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth}}
body{{font-family:'SF Mono',ui-monospace,'Cascadia Code',monospace;font-size:13px;background:var(--bg);color:var(--text);min-height:100vh}}

.shell{{display:flex;min-height:100vh}}
.sidebar{{width:210px;flex-shrink:0;background:var(--surface);border-right:1px solid var(--border);padding:24px 0;position:sticky;top:0;height:100vh;overflow-y:auto;display:flex;flex-direction:column;gap:2px;box-shadow:2px 0 8px rgba(0,0,0,.04)}}
.sidebar-logo{{padding:0 18px 18px;font-size:14px;font-weight:700;color:var(--accent);letter-spacing:-.02em;border-bottom:1px solid var(--border);margin-bottom:10px}}
.sidebar-logo span{{color:var(--text-muted);font-weight:400}}
.nav-item{{display:flex;align-items:center;gap:9px;padding:7px 18px;color:var(--text-muted);text-decoration:none;font-size:12px;transition:all .13s;border-left:2px solid transparent}}
.nav-item:hover,.nav-item.active{{color:var(--accent);background:var(--surface2);border-left-color:var(--accent)}}
.nav-dot{{width:5px;height:5px;border-radius:50%;background:currentColor;flex-shrink:0}}
.main{{flex:1;padding:36px 40px;max-width:1140px}}

.page-header{{margin-bottom:32px}}
.page-title{{font-size:21px;font-weight:700;color:var(--text);letter-spacing:-.03em}}
.run-meta{{margin-top:6px;color:var(--text-muted);font-size:11px}}
.run-meta code{{color:var(--accent);background:var(--surface2);padding:2px 7px;border-radius:4px;border:1px solid var(--border)}}

.stat-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(148px,1fr));gap:12px;margin-bottom:28px}}
.stat-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;position:relative;overflow:hidden;transition:border-color .18s,transform .18s,box-shadow .18s;box-shadow:0 1px 4px rgba(0,0,0,.05)}}
.stat-card:hover{{border-color:var(--accent);transform:translateY(-2px);box-shadow:0 4px 16px rgba(79,70,229,.1)}}
.stat-card::before{{content:'';position:absolute;inset:0;background:linear-gradient(135deg,var(--accent) 0%,transparent 55%);opacity:.03}}
.stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:7px}}
.stat-value{{font-size:23px;font-weight:700;color:var(--text);line-height:1}}
.stat-sub{{font-size:10px;color:var(--text-muted);margin-top:3px}}

.section{{margin-bottom:34px}}
.section-title{{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:var(--text-muted);margin-bottom:14px;display:flex;align-items:center;gap:10px}}
.section-title::after{{content:'';flex:1;height:1px;background:var(--border)}}

.chart-grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-bottom:18px}}
.chart-card{{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;box-shadow:0 1px 4px rgba(0,0,0,.05)}}
.chart-card-title{{font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:14px}}
.chart-wrap{{position:relative}}

.hist-tabs{{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:12px}}
.hist-tab{{padding:3px 10px;border-radius:4px;font-size:11px;cursor:pointer;background:var(--surface2);border:1px solid var(--border);color:var(--text-muted);transition:all .13s;font-family:inherit}}
.hist-tab.active,.hist-tab:hover{{background:var(--accent);border-color:var(--accent);color:#fff}}

.table-wrap{{overflow-x:auto;border-radius:var(--radius);border:1px solid var(--border);box-shadow:0 1px 4px rgba(0,0,0,.05)}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{text-align:left;padding:9px 13px;color:var(--text-muted);font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;background:var(--surface2);border-bottom:1px solid var(--border)}}
td{{padding:8px 13px;border-bottom:1px solid var(--border);color:var(--text);vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
.col-row{{transition:background .1s}}
.col-row:hover{{background:var(--surface2)}}
.col-name{{font-weight:600;color:var(--accent2)}}
.num{{text-align:right;font-variant-numeric:tabular-nums;color:var(--text-muted)}}
.dtype-chip{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;background:var(--surface2);color:var(--text-muted);border:1px solid var(--border)}}
.null-bar-wrap{{display:flex;align-items:center;gap:8px;min-width:110px}}
.null-bar-fill{{height:4px;border-radius:2px;min-width:2px;transition:width .4s ease}}

.badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700;letter-spacing:.04em}}
.badge-error{{background:rgba(220,38,38,.10);color:var(--red);border:1px solid rgba(220,38,38,.22)}}
.badge-warn{{background:rgba(217,119,6,.10);color:var(--yellow);border:1px solid rgba(217,119,6,.22)}}
.badge-info{{background:rgba(79,70,229,.10);color:var(--accent);border:1px solid rgba(79,70,229,.22)}}
.badge-ok{{background:transparent;color:var(--text-muted);border:none}}

@keyframes fadeUp{{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
.anim{{animation:fadeUp .38s ease both}}
.anim-1{{animation-delay:.04s}}.anim-2{{animation-delay:.09s}}.anim-3{{animation-delay:.14s}}
.anim-4{{animation-delay:.19s}}.anim-5{{animation-delay:.24s}}

.empty{{color:var(--text-muted);padding:24px;text-align:center;font-size:12px}}

@media(max-width:860px){{
  .sidebar{{display:none}}
  .chart-grid{{grid-template-columns:1fr}}
  .main{{padding:20px 16px}}
}}
</style>
</head>
<body>
<div class="shell">

<nav class="sidebar">
  <div class="sidebar-logo">datapill <span>profile</span></div>
  <a class="nav-item active" href="#overview"><span class="nav-dot"></span>Overview</a>
  <a class="nav-item" href="#charts"><span class="nav-dot"></span>Charts</a>
  <a class="nav-item" href="#columns"><span class="nav-dot"></span>Columns</a>
  {corr_nav}
  {warn_nav}
</nav>

<main class="main">

  <div class="page-header anim">
    <div class="page-title">Profile Report</div>
    <div class="run-meta">run <code>{run_id}</code>&nbsp;&nbsp;parent <code>{parent_run_id}</code></div>
  </div>

  <div id="overview">
    <div class="section-title">Dataset overview</div>
    <div class="stat-grid">
      <div class="stat-card anim anim-1">
        <div class="stat-label">Rows</div>
        <div class="stat-value">{n_rows}</div>
      </div>
      <div class="stat-card anim anim-2">
        <div class="stat-label">Columns</div>
        <div class="stat-value">{n_columns}</div>
      </div>
      <div class="stat-card anim anim-3">
        <div class="stat-label">Memory</div>
        <div class="stat-value">{memory_mb}</div>
        <div class="stat-sub">MB</div>
      </div>
      <div class="stat-card anim anim-4">
        <div class="stat-label">Null %</div>
        <div class="stat-value" style="color:{null_total_color}">{total_null_pct}</div>
      </div>
      <div class="stat-card anim anim-5">
        <div class="stat-label">Duplicate %</div>
        <div class="stat-value">{duplicate_pct}</div>
      </div>
      <div class="stat-card anim anim-5">
        <div class="stat-label">Warnings</div>
        <div class="stat-value" style="color:{warn_value_color}">{n_warnings}</div>
        <div class="stat-sub">{n_errors} errors · {n_warns} warns</div>
      </div>
    </div>
  </div>

  <div id="charts" class="section">
    <div class="section-title">Visual analysis</div>
    <div class="chart-grid">
      <div class="chart-card anim anim-1">
        <div class="chart-card-title">Null % by column</div>
        <div class="chart-wrap" style="height:200px"><canvas id="nullChart"></canvas></div>
      </div>
      <div class="chart-card anim anim-2">
        <div class="chart-card-title">Column type distribution</div>
        <div class="chart-wrap" style="height:200px"><canvas id="typeChart"></canvas></div>
      </div>
    </div>

    <div id="histSection" style="display:none" class="chart-card anim anim-3" style="margin-bottom:18px">
      <div class="chart-card-title">Numeric distributions</div>
      <div class="hist-tabs" id="histTabs"></div>
      <div class="chart-wrap" style="height:190px"><canvas id="histChart"></canvas></div>
    </div>
  </div>

  <div id="columns" class="section">
    <div class="section-title">Columns ({n_columns_label})</div>
    <div class="table-wrap anim">
      <table>
        <thead>
          <tr>
            <th>Column</th><th>Type</th><th>Null %</th>
            <th style="text-align:right">Distinct</th>
            <th style="text-align:right">Mean / top value</th>
            <th style="text-align:center">Warns</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
  </div>

  {corr_section_html}
  {warn_section_html}

</main>
</div>

<script>
Chart.defaults.color = '#6b7280';
Chart.defaults.borderColor = '#dde1ea';
Chart.defaults.font.family = "'SF Mono', ui-monospace, 'Cascadia Code', monospace";
Chart.defaults.font.size = 11;

const ACCENT = '#4f46e5', ACCENT2 = '#0891b2', GREEN = '#059669',
      YELLOW = '#d97706', RED = '#dc2626', MAGENTA = '#9333ea';
const PALETTE = [ACCENT, ACCENT2, GREEN, YELLOW, MAGENTA, RED, '#ea580c', '#7c3aed'];

(function() {{
  const labels = {null_chart_labels};
  const values = {null_chart_values};
  if (!labels.length) return;
  new Chart(document.getElementById('nullChart'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label: 'Null %',
        data: values,
        backgroundColor: values.map(v =>
          v > 30 ? 'rgba(220,38,38,0.75)' : v > 10 ? 'rgba(217,119,6,0.70)' : 'rgba(79,70,229,0.65)'
        ),
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      animation: {{ duration: 800, easing: 'easeOutQuart' }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#ffffff', borderColor: '#dde1ea', borderWidth: 1,
          titleColor: '#1a1f2e', bodyColor: '#6b7280',
          callbacks: {{ label: ctx => ` ${{ctx.raw.toFixed(2)}}% null` }}
        }}
      }},
      scales: {{
        x: {{ ticks: {{ maxRotation: 45, maxTicksLimit: 20 }}, grid: {{ display: false }} }},
        y: {{ max: Math.min(100, Math.max(...values, 5) * 1.2), ticks: {{ callback: v => v + '%' }} }}
      }}
    }}
  }});
}})();

(function() {{
  const labels = {col_type_labels};
  const values = {col_type_values};
  new Chart(document.getElementById('typeChart'), {{
    type: 'doughnut',
    data: {{
      labels,
      datasets: [{{
        data: values,
        backgroundColor: [ACCENT, MAGENTA, GREEN, '#94a3b8'],
        borderColor: '#ffffff',
        borderWidth: 3,
        hoverOffset: 8,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      animation: {{ duration: 900, easing: 'easeOutBack' }},
      cutout: '64%',
      plugins: {{
        legend: {{ position: 'right', labels: {{ padding: 14, boxWidth: 12, boxHeight: 12, borderRadius: 3 }} }},
        tooltip: {{
          backgroundColor: '#ffffff', borderColor: '#dde1ea', borderWidth: 1,
          titleColor: '#1a1f2e', bodyColor: '#6b7280',
        }}
      }}
    }}
  }});
}})();

(function() {{
  const histData = {hist_data_js};
  if (!histData || !histData.length) return;
  document.getElementById('histSection').style.display = 'block';
  const tabsEl = document.getElementById('histTabs');
  let currentChart = null;

  function renderHist(idx) {{
    if (currentChart) currentChart.destroy();
    const d = histData[idx];
    const color = PALETTE[idx % PALETTE.length];
    currentChart = new Chart(document.getElementById('histChart'), {{
      type: 'bar',
      data: {{
        labels: d.labels,
        datasets: [{{
          label: d.name,
          data: d.counts,
          backgroundColor: color + 'aa',
          borderColor: color,
          borderWidth: 1,
          borderRadius: 3,
          borderSkipped: false,
        }}]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        animation: {{ duration: 450, easing: 'easeOutQuart' }},
        plugins: {{
          legend: {{ display: false }},
          tooltip: {{
            backgroundColor: '#ffffff', borderColor: '#dde1ea', borderWidth: 1,
            titleColor: '#1a1f2e', bodyColor: '#6b7280',
            callbacks: {{ title: ctx => d.name + ' · bin ' + ctx[0].label }}
          }}
        }},
        scales: {{
          x: {{ grid: {{ display: false }}, ticks: {{ maxTicksLimit: 12 }} }},
          y: {{ ticks: {{ precision: 0 }} }}
        }}
      }}
    }});
  }}

  histData.forEach((d, i) => {{
    const tab = document.createElement('button');
    tab.className = 'hist-tab' + (i === 0 ? ' active' : '');
    tab.textContent = d.name;
    tab.onclick = () => {{
      tabsEl.querySelectorAll('.hist-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderHist(i);
    }};
    tabsEl.appendChild(tab);
  }});
  renderHist(0);
}})();

(function() {{
  const el = document.getElementById('corrChart');
  if (!el) return;
  const labels = {corr_labels_js};
  const values = {corr_values_js};
  const colors = {corr_colors_js};
  if (!labels.length) return;
  new Chart(el, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label: 'r',
        data: values,
        backgroundColor: colors,
        borderRadius: 4,
        borderSkipped: false,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      animation: {{ duration: 700, easing: 'easeOutQuart' }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#ffffff', borderColor: '#dde1ea', borderWidth: 1,
          titleColor: '#1a1f2e', bodyColor: '#6b7280',
          callbacks: {{ label: ctx => ` r = ${{ctx.raw >= 0 ? '+' : ''}}${{ctx.raw.toFixed(4)}}` }}
        }}
      }},
      scales: {{
        x: {{ min: -1, max: 1, ticks: {{ callback: v => (v > 0 ? '+' : '') + v }} }},
        y: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }} }} }}
      }}
    }}
  }});
}})();

(function() {{
  const items = document.querySelectorAll('.nav-item');
  const observer = new IntersectionObserver(entries => {{
    entries.forEach(e => {{
      if (e.isIntersecting) {{
        items.forEach(a => a.classList.remove('active'));
        const link = document.querySelector('.nav-item[href="#' + e.target.id + '"]');
        if (link) link.classList.add('active');
      }}
    }});
  }}, {{ threshold: 0.25 }});
  document.querySelectorAll('[id]').forEach(el => observer.observe(el));
}})();
</script>
</body>
</html>"""