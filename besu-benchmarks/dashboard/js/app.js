let indexData = null;
function escapeHtml(unsafe) {
  if (typeof unsafe !== 'string') return unsafe;
  return unsafe
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
function escapeJS(unsafe) {
  if (typeof unsafe !== 'string') return unsafe;
  return unsafe
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'")
    .replace(/"/g, '\\"')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r');
}
function escapeHtmlJs(unsafe) {
  return escapeHtml(escapeJS(unsafe));
}
document.addEventListener('DOMContentLoaded', async () => {
  try {
    const idxRes = await fetch('data/index.json');
    indexData = await idxRes.json();
    renderHero();
    renderOverview();
    renderBenchTable();
  } catch (err) {
    console.error('Failed to load dashboard data:', err);
    document.getElementById('footer-updated').textContent = 'Error loading data';
  }
});

function renderHero() {
  document.getElementById('val-runs').textContent = indexData.total_runs;
  document.getElementById('val-benchmarks').textContent = indexData.total_benchmarks;
  if (indexData.runs && indexData.runs.length > 0) {
    const latestRun = indexData.runs[0];
    document.getElementById('val-latest-ref').innerHTML = `
      ${escapeHtml(latestRun.besu_ref)} <span style="font-size:0.6em;color:var(--text-muted)">(${escapeHtml(latestRun.besu_sha.substring(0,7))})</span>
    `;
  }
  const rc = indexData.regressions.length;
  const elReg = document.getElementById('val-regressions');
  elReg.textContent = rc;
  if (rc > 0) {
    document.getElementById('stat-regressions').classList.add('has-regressions');
  }
  const d = new Date(indexData.generated_at);
  document.getElementById('footer-updated').textContent = d.toLocaleString();
}
function renderOverview() {
  // Regressions
  if (indexData.regressions && indexData.regressions.length > 0) {
    const wrap = document.getElementById('regression-alerts');
    wrap.classList.remove('hidden');
    let html = `<h3>⚠️ Action Required: ${indexData.regressions.length} Performance Regression(s) Detected</h3><ul>`;
    indexData.regressions.forEach(r => {
      html += `<li><strong>${escapeHtml(r.name)}</strong> dropped by <span style="font-family:var(--mono);font-weight:bold">${r.pct_change.toFixed(1)}%</span> (from ${r.prev_score.toFixed(1)} to ${r.curr_score.toFixed(1)} ${escapeHtml(r.unit)}) in ref <code>${escapeHtml(r.curr_ref)}</code>.</li>`;
    });
    html += `</ul>`;
    wrap.innerHTML = html;
  }
  // Category cards
  const cats = {};
  indexData.latest.forEach(b => {
    if (!cats[b.category]) cats[b.category] = { count: 0, best: null };
    cats[b.category].count++;
    if (!cats[b.category].best || b.score > cats[b.category].best.score) {
      cats[b.category].best = b;
    }
  });
  const grid = document.getElementById('category-grid');
  let catHtml = '';
  Object.keys(cats).sort().forEach(c => {
    const d = cats[c];
    catHtml += `
      <div class="cat-card cat-${escapeHtml(c)}" onclick="document.querySelector('.nav-link[data-tab=\\'benchmarks\\']').click(); setTimeout(()=>filterCategory('${escapeHtmlJs(c)}'),10)">
        <div class="cat-name">${escapeHtml(c)}</div>
        <div class="cat-count">${d.count} benchmarks</div>
        <div class="cat-best">Top: ${escapeHtml(d.best.name)} (${d.best.score.toFixed(0)} ${escapeHtml(d.best.score_unit)})</div>
      </div>
    `;
  });
  grid.innerHTML = catHtml;
}
function renderBenchTable() {
  const tbody = document.getElementById('bench-tbody');
  let html = '';
  indexData.latest.forEach(b => {
    let pctCls = 'change-neutral';
    let pctTxt = '—';
    html += `
      <tr>
        <td>
          <a href="#" class="bench-name">${escapeHtml(b.name)}</a>
        </td>
        <td><span class="bench-cat">${escapeHtml(b.category)}</span></td>
        <td class="bench-score">${b.score.toFixed(1)}</td>
        <td class="bench-unit">${escapeHtml(b.score_unit)}</td>
        <td class="${pctCls}">${pctTxt}</td>
        <td></td>
      </tr>
    `;
  });
  tbody.innerHTML = html;
}

