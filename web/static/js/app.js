/**
 * Real-Time Streaming Operations Center - Client Application
 * Handles WebSockets, dynamic charts, live table feeds, interactive chaos actions, and DLQ inspection.
 */

// Application State
const state = {
  activeTab: 'avro',
  runningAvgHistory: [],
  maxChartPoints: 40,
  ws: null,
  reconnectAttempts: 0
};

// ---------------------------------------------------------------------------
// Tab Management
// ---------------------------------------------------------------------------
function switchTab(tab) {
  state.activeTab = tab;
  const avroContent = document.getElementById('tab-avro-content');
  const gridContent = document.getElementById('tab-grid-content');
  const btnAvro = document.getElementById('tab-btn-avro');
  const btnGrid = document.getElementById('tab-btn-grid');

  if (tab === 'avro') {
    avroContent.style.display = 'block';
    gridContent.style.display = 'none';
    btnAvro.classList.add('active');
    btnAvro.setAttribute('aria-selected', 'true');
    btnGrid.classList.remove('active');
    btnGrid.setAttribute('aria-selected', 'false');
  } else {
    avroContent.style.display = 'none';
    gridContent.style.display = 'block';
    btnGrid.classList.add('active');
    btnGrid.setAttribute('aria-selected', 'true');
    btnAvro.classList.remove('active');
    btnAvro.setAttribute('aria-selected', 'false');
  }
}

// ---------------------------------------------------------------------------
// WebSocket Connection & Event Dispatcher
// ---------------------------------------------------------------------------
function initWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/stream`;

  const badge = document.getElementById('ws-status-badge');
  const dot = document.getElementById('ws-status-dot');
  const statusText = document.getElementById('ws-status-text');

  state.ws = new WebSocket(wsUrl);

  state.ws.onopen = () => {
    state.reconnectAttempts = 0;
    dot.style.backgroundColor = 'var(--accent-emerald)';
    dot.style.boxShadow = '0 0 8px var(--accent-emerald)';
    statusText.innerText = 'LIVE STREAMING';
    badge.style.borderColor = 'hsla(156, 73%, 48%, 0.3)';
    console.log('[WebSocket] Connected to streaming hub.');
  };

  state.ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleStreamPayload(data);
    } catch (err) {
      console.error('[WebSocket] Error parsing message:', err);
    }
  };

  state.ws.onclose = () => {
    dot.style.backgroundColor = 'var(--accent-rose)';
    dot.style.boxShadow = '0 0 8px var(--accent-rose)';
    statusText.innerText = 'DISCONNECTED';
    badge.style.borderColor = 'hsla(351, 95%, 63%, 0.3)';

    // Exponential backoff reconnect
    const timeout = Math.min(10000, 1000 * Math.pow(1.5, state.reconnectAttempts));
    state.reconnectAttempts++;
    console.warn(`[WebSocket] Disconnected. Reconnecting in ${timeout / 1000}s...`);
    setTimeout(initWebSocket, timeout);
  };

  state.ws.onerror = (err) => {
    console.error('[WebSocket] Error:', err);
    state.ws.close();
  };
}

function handleStreamPayload(data) {
  if (data.type === 'INIT_STATE') {
    updateKPIs(data.order_stats);
    renderInitialOrders(data.recent_orders || []);
    renderDLQTable(data.dlq_items || []);
    if (data.grid) updateGridTab(data.grid);
    if (data.auto_stream !== undefined) {
      document.getElementById('toggle-stream').checked = data.auto_stream;
    }
  } else if (data.type === 'STREAM_TICK') {
    if (data.order_stats) updateKPIs(data.order_stats);
    if (data.order) {
      prependOrderRow(data.order);
      recordChartPoint(data.order.running_avg);
    }
    if (data.grid) updateGridTab(data.grid);
  } else if (data.type === 'MANUAL_ORDER') {
    prependOrderRow(data.order, true);
    recordChartPoint(data.order.running_avg);
    showToast(`Order ${data.order.orderId} processed (${data.order.status})`, data.order.status === 'SUCCESS' ? 'success' : 'warning');
  } else if (data.type === 'DLQ_ALERT') {
    prependOrderRow(data.order, true);
    if (data.dlq_record) appendDLQRow(data.dlq_record);
    showToast(`POISON PILL DETECTED! Quarantined to DLQ: ${data.order.orderId}`, 'error');
  } else if (data.type === 'DLQ_REPLAYED') {
    prependOrderRow(data.order, true);
    markDLQReplayed(data.dlq_id);
    showToast(`DLQ Message ${data.dlq_id} successfully repaired and replayed!`, 'success');
  }
}

// ---------------------------------------------------------------------------
// KPI Updating
// ---------------------------------------------------------------------------
function updateKPIs(stats) {
  if (!stats) return;
  document.getElementById('kpi-total-orders').innerText = stats.total_orders.toLocaleString();
  document.getElementById('kpi-running-avg').innerText = `$${Number(stats.running_avg).toFixed(2)}`;
  document.getElementById('kpi-retries').innerText = stats.retries_count || 0;
  document.getElementById('kpi-dlq').innerText = stats.dlq_count || 0;
  document.getElementById('chart-latest-avg').innerText = `Latest Avg: $${Number(stats.running_avg).toFixed(2)}`;
}

// ---------------------------------------------------------------------------
// Table Rendering: Orders Feed
// ---------------------------------------------------------------------------
function renderInitialOrders(orders) {
  const tbody = document.getElementById('orders-tbody');
  tbody.innerHTML = '';
  orders.forEach(order => {
    prependOrderRow(order, false);
    recordChartPoint(order.running_avg);
  });
}

function prependOrderRow(order, isHighlight = false) {
  const tbody = document.getElementById('orders-tbody');
  const tr = document.createElement('tr');

  let statusBadge = '';
  if (order.status === 'SUCCESS') {
    statusBadge = `<span class="badge-status success">&#10003; Processed</span>`;
  } else if (order.status === 'RECOVERED_AFTER_RETRY') {
    statusBadge = `<span class="badge-status retry">&#9888; Retried (${order.attempts}/3)</span>`;
  } else if (order.status === 'DLQ_QUARANTINED') {
    statusBadge = `<span class="badge-status dlq">&#10007; Quarantined</span>`;
  }

  const priceColor = order.price < 0 ? 'var(--accent-rose)' : 'var(--accent-cyan)';
  const formattedPrice = `$${Number(order.price).toFixed(2)}`;

  tr.innerHTML = `
    <td style="font-weight: 600; color: var(--text-main);">${order.orderId}</td>
    <td style="color: var(--text-muted);">${order.product}</td>
    <td style="color: ${priceColor}; font-weight: 600;">${formattedPrice}</td>
    <td><span style="font-family: var(--font-mono);">${order.attempts}</span></td>
    <td style="color: var(--accent-emerald); font-weight: 600;">$${Number(order.running_avg).toFixed(2)}</td>
    <td style="color: var(--text-faint);">${order.duration_ms || 12} ms</td>
    <td>${statusBadge}</td>
  `;

  if (isHighlight) {
    tr.style.backgroundColor = 'hsla(217, 91%, 60%, 0.2)';
    setTimeout(() => {
      tr.style.transition = 'background-color 1s ease';
      tr.style.backgroundColor = '';
    }, 1200);
  }

  tbody.insertBefore(tr, tbody.firstChild);

  // Keep table bounded to prevent memory leaks
  if (tbody.children.length > 35) {
    tbody.removeChild(tbody.lastChild);
  }
}

// ---------------------------------------------------------------------------
// Dead Letter Queue (DLQ) Inspector
// ---------------------------------------------------------------------------
function renderDLQTable(items) {
  const tbody = document.getElementById('dlq-tbody');
  const badge = document.getElementById('dlq-table-badge');
  badge.innerText = `${items.length} Poison Messages`;

  if (!items || items.length === 0) {
    tbody.innerHTML = `
      <tr>
        <td colspan="5" style="text-align: center; color: var(--text-muted); padding: 1.5rem;">
          No messages in DLQ. Inject a poison pill to test dead letter routing!
        </td>
      </tr>
    `;
    return;
  }

  tbody.innerHTML = '';
  items.slice().reverse().forEach(item => {
    appendDLQRow(item);
  });
}

function appendDLQRow(item) {
  const tbody = document.getElementById('dlq-tbody');
  // Remove placeholder if present
  if (tbody.children.length === 1 && tbody.children[0].innerText.includes('No messages')) {
    tbody.innerHTML = '';
  }

  const tr = document.createElement('tr');
  tr.id = `dlq-row-${item.dlq_id}`;

  const failedMsg = item.failed_message || {};
  const orderId = failedMsg.orderId || 'N/A';
  const price = failedMsg.price !== undefined ? `$${Number(failedMsg.price).toFixed(2)}` : 'N/A';
  const dateStr = new Date(item.timestamp * 1000).toLocaleTimeString();

  const actionHtml = item.replayed
    ? `<span style="color: var(--accent-emerald); font-weight: 600; font-size: 0.75rem;">&#10003; Replayed</span>`
    : `<button class="action-btn btn-glitch" style="padding: 0.25rem 0.65rem; font-size: 0.75rem;" onclick="replayDLQ('${item.dlq_id}')">Re-drive</button>`;

  tr.innerHTML = `
    <td style="color: var(--accent-rose); font-weight: 700;">${item.dlq_id}</td>
    <td>${orderId} (${price})</td>
    <td style="color: var(--text-muted); max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${item.error_reason}">${item.error_reason}</td>
    <td style="color: var(--text-faint);">${dateStr}</td>
    <td>${actionHtml}</td>
  `;

  tbody.insertBefore(tr, tbody.firstChild);
}

function markDLQReplayed(dlqId) {
  const row = document.getElementById(`dlq-row-${dlqId}`);
  if (row) {
    const actionTd = row.children[4];
    actionTd.innerHTML = `<span style="color: var(--accent-emerald); font-weight: 600; font-size: 0.75rem;">&#10003; Replayed</span>`;
  }
}

// ---------------------------------------------------------------------------
// Dynamic SVG Sparkline Chart
// ---------------------------------------------------------------------------
function recordChartPoint(value) {
  if (value === undefined || value === null || isNaN(value)) return;
  state.runningAvgHistory.push(Number(value));
  if (state.runningAvgHistory.length > state.maxChartPoints) {
    state.runningAvgHistory.shift();
  }
  renderChart();
}

function renderChart() {
  const svg = document.getElementById('sparkline-svg');
  const data = state.runningAvgHistory;
  if (data.length < 2) return;

  const width = 700;
  const height = 180;
  const paddingY = 25;

  const minVal = Math.min(...data) * 0.95;
  const maxVal = Math.max(...data) * 1.05;
  const range = maxVal - minVal || 1;

  const points = data.map((val, idx) => {
    const x = (idx / (data.length - 1)) * width;
    const y = height - paddingY - ((val - minVal) / range) * (height - 2 * paddingY);
    return { x, y, val };
  });

  const pathD = points.reduce((acc, pt, i) => `${acc} ${i === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`, '');
  const areaD = `${pathD} L ${width} ${height} L 0 ${height} Z`;

  svg.innerHTML = `
    <defs>
      <linearGradient id="chartGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="hsl(188, 94%, 53%)" stop-opacity="0.35"/>
        <stop offset="100%" stop-color="hsl(217, 91%, 60%)" stop-opacity="0.0"/>
      </linearGradient>
    </defs>
    <!-- Background grid line -->
    <line x1="0" y1="${height/2}" x2="${width}" y2="${height/2}" stroke="hsla(220, 20%, 30%, 0.3)" stroke-dasharray="4"/>
    <!-- Filled area -->
    <path d="${areaD}" fill="url(#chartGradient)" />
    <!-- Main line -->
    <path d="${pathD}" fill="none" stroke="hsl(188, 94%, 53%)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
    <!-- Latest pulse point -->
    <circle cx="${points[points.length - 1].x}" cy="${points[points.length - 1].y}" r="5" fill="hsl(156, 73%, 48%)" stroke="#ffffff" stroke-width="2"/>
  `;
}

// ---------------------------------------------------------------------------
// Smart Grid Tab Visualizer
// ---------------------------------------------------------------------------
function updateGridTab(grid) {
  if (!grid) return;
  document.getElementById('grid-sim-day').innerText = `Day #${grid.simulated_day}`;
  document.getElementById('grid-batch-num').innerText = `Batch #${grid.batch_num}`;
  document.getElementById('grid-total-events').innerText = grid.total_events.toLocaleString();
  document.getElementById('grid-db-badge').innerText = `Database Sink: ${grid.db_sink}`;

  const container = document.getElementById('zones-container');
  if (!grid.zones || grid.zones.length === 0) return;

  container.innerHTML = grid.zones.map(z => {
    const isFeed = z.net_load_kwh < 0;
    const netClass = isFeed ? 'feed' : 'draw';
    const netLabel = isFeed ? `${z.net_load_kwh.toFixed(2)} kWh (FEED)` : `+${z.net_load_kwh.toFixed(2)} kWh (DRAW)`;

    return `
      <div class="zone-card">
        <div class="zone-card-top">
          <span class="zone-name">${z.grid_zone}</span>
          <span class="net-load-pill ${netClass}">${netLabel}</span>
        </div>
        <div class="zone-stats-list">
          <div class="zone-stat-row">
            <span>Consumption:</span>
            <strong>${Number(z.total_consumption_kwh).toFixed(2)} kWh</strong>
          </div>
          <div class="zone-stat-row">
            <span>Solar Generation:</span>
            <strong style="color: var(--accent-emerald);">${Number(z.total_solar_generation_kwh).toFixed(2)} kWh</strong>
          </div>
          <div class="zone-stat-row">
            <span>Renewable Penetration:</span>
            <strong style="color: var(--accent-cyan);">${Number(z.renewable_contribution_pct).toFixed(1)}%</strong>
          </div>
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width: ${Math.min(100, z.renewable_contribution_pct)}%;"></div>
          </div>
          <div class="zone-stat-row" style="margin-top: 0.5rem;">
            <span>Estimated Billing:</span>
            <strong>$${Number(z.total_cost_estimate).toFixed(2)}</strong>
          </div>
        </div>
      </div>
    `;
  }).join('');
}

// ---------------------------------------------------------------------------
// Interactive Chaos & Marker Actions
// ---------------------------------------------------------------------------
async function sendStandardOrder() {
  try {
    const res = await fetch('/api/orders/produce', { method: 'POST' });
    const data = await res.json();
    console.log('Valid order produced:', data);
  } catch (err) {
    showToast('Failed to produce order: ' + err, 'error');
  }
}

async function injectGlitch() {
  try {
    const res = await fetch('/api/orders/inject-glitch', { method: 'POST' });
    const data = await res.json();
    console.log('Glitch injected:', data);
  } catch (err) {
    showToast('Failed to inject glitch: ' + err, 'error');
  }
}

async function injectPoison() {
  try {
    const res = await fetch('/api/orders/inject-poison', { method: 'POST' });
    const data = await res.json();
    console.log('Poison injected:', data);
  } catch (err) {
    showToast('Failed to inject poison pill: ' + err, 'error');
  }
}

async function replayDLQ(dlqId) {
  try {
    const res = await fetch('/api/orders/dlq/replay', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dlq_id: dlqId })
    });
    const data = await res.json();
    console.log('DLQ Replayed:', data);
  } catch (err) {
    showToast('Failed to replay DLQ: ' + err, 'error');
  }
}

async function toggleAutoStream(checked) {
  try {
    await fetch(`/api/stream/toggle?active=${checked}`, { method: 'POST' });
    showToast(`Continuous generator ${checked ? 'resumed' : 'paused'}`, checked ? 'success' : 'warning');
  } catch (err) {
    showToast('Failed to toggle stream: ' + err, 'error');
  }
}

async function changeSpeed(val) {
  try {
    await fetch(`/api/stream/toggle?speed=${val}`, { method: 'POST' });
    showToast(`Stream interval set to ${val}s`, 'success');
  } catch (err) {
    showToast('Failed to change speed: ' + err, 'error');
  }
}

// ---------------------------------------------------------------------------
// Toast Notification Utility
// ---------------------------------------------------------------------------
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerText = message;

  container.appendChild(toast);
  setTimeout(() => {
    toast.style.transition = 'opacity 0.4s ease, transform 0.4s ease';
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    setTimeout(() => toast.remove(), 400);
  }, 3500);
}

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', () => {
  initWebSocket();
});
