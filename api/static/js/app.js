// Shopee Flash-Sale Dashboard & SRE Operations Client

let catalog = [];
let activeItem = null;
let token = null;
let eventSource = null;
let currentUserId = "550e8400-e29b-41d4-a716-446655440001";

const ITEM_IMAGES = {
    "SHOPEE-1111-IPHONE16-MAX": "https://images.unsplash.com/photo-1592750475338-74b7b21085ab?w=600&auto=format&fit=crop&q=80",
    "AGODA-MALDIVES-VILLA-DROP": "https://images.unsplash.com/photo-1514282401047-d79a71a590e8?w=600&auto=format&fit=crop&q=80",
    "CAREEM-DUBAI-SUPERCAR-PASS": "https://images.unsplash.com/photo-1542282088-72c9c27ed0cd?w=600&auto=format&fit=crop&q=80",
    "CONCURRENCY-STRESS-BENCHMARK-ITEM": "https://images.unsplash.com/photo-1526374965328-7f61d4dc18c5?w=600&auto=format&fit=crop&q=80"
};

// Initialize Lucide Icons
lucide.createIcons();

// Initialize 100 benchmark matrix dots
const dotGrid = document.getElementById('dot-grid');
if (dotGrid) {
    for (let i = 0; i < 100; i++) {
        const d = document.createElement('div');
        d.className = 'h-3 bg-gray-200 rounded-xs transition-colors duration-200';
        d.id = `dot-${i}`;
        dotGrid.appendChild(d);
    }
}

// Tab Switching
const tabStorefront = document.getElementById('tab-storefront');
const tabOps = document.getElementById('tab-ops');
const viewStorefront = document.getElementById('view-storefront');
const viewOps = document.getElementById('view-ops');

if (tabStorefront && tabOps) {
    tabStorefront.addEventListener('click', () => {
        viewStorefront.classList.remove('hidden');
        viewOps.classList.add('hidden');
        tabStorefront.className = 'px-3.5 py-1.5 bg-white text-shopee-orange font-bold rounded-sm shadow-sm flex items-center gap-1.5 transition-all';
        tabOps.className = 'px-3.5 py-1.5 text-white/90 hover:text-white font-medium rounded-sm flex items-center gap-1.5 transition-all';
    });

    tabOps.addEventListener('click', () => {
        viewOps.classList.remove('hidden');
        viewStorefront.classList.add('hidden');
        tabOps.className = 'px-3.5 py-1.5 bg-white text-shopee-orange font-bold rounded-sm shadow-sm flex items-center gap-1.5 transition-all';
        tabStorefront.className = 'px-3.5 py-1.5 text-white/90 hover:text-white font-medium rounded-sm flex items-center gap-1.5 transition-all';
    });
}

function appendLog(msg) {
    const term = document.getElementById('log-terminal');
    if (!term) return;
    const time = new Date().toISOString().substring(11, 19);
    const line = document.createElement('div');
    line.innerHTML = `<span class="text-gray-400">[${time}]</span> ${msg}`;
    term.appendChild(line);
    term.scrollTop = term.scrollHeight;
}

// Fetch Auth Token for Active Persona
async function updatePersonaToken() {
    const persona = document.getElementById('persona-select').value;
    try {
        const res = await fetch('/api/v1/auth/demo-token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ persona })
        });
        const data = await res.json();
        token = data.access_token;
        currentUserId = data.user_id;
        appendLog(`Shopper authenticated: <strong class="text-yellow-400">${data.name}</strong> (${data.tier})`);
    } catch (err) {
        console.error("Auth token error", err);
    }
}

// Load Catalog
async function loadCatalog() {
    try {
        const res = await fetch('/api/v1/items');
        catalog = await res.json();
        const picker = document.getElementById('item-picker');
        picker.innerHTML = '';
        catalog.forEach(item => {
            const opt = document.createElement('option');
            opt.value = item.id;
            opt.textContent = `${item.title.substring(0, 42)}... ($${(item.price_cents / 100).toFixed(2)})`;
            picker.appendChild(opt);
        });

        const shopeeItem = catalog.find(x => x.sku === "SHOPEE-1111-IPHONE16-MAX") || catalog[0];
        if (shopeeItem) {
            picker.value = shopeeItem.id;
            selectItem(shopeeItem);
        }
    } catch (err) {
        console.error("Failed to load catalog", err);
    }
}

function selectItem(item) {
    activeItem = item;
    document.getElementById('item-title').textContent = item.title;
    const price = (item.price_cents / 100).toFixed(2);
    document.getElementById('item-price').textContent = `$${price}`;
    document.getElementById('item-original-price').textContent = `$${(item.price_cents * 10 / 100).toFixed(2)}`;

    if (ITEM_IMAGES[item.sku]) {
        document.getElementById('item-image').src = ITEM_IMAGES[item.sku];
    }

    updateStockDisplay(item.available_stock, item.total_stock);
    connectSSE(item.id);
}

function updateStockDisplay(available, total) {
    const pct = Math.max(0, Math.min(100, Math.round((available / total) * 100)));
    const bar = document.getElementById('stock-progress');
    bar.style.width = `${pct}%`;
    document.getElementById('stock-label').textContent = `${available} / ${total} AVAILABLE`;
    document.getElementById('stock-percentage').textContent = `${pct}% Stock`;

    const btn = document.getElementById('reserve-btn');
    const btnText = document.getElementById('btn-text');
    const soldOutOverlay = document.getElementById('sold-out-overlay');

    if (available <= 0) {
        btn.disabled = true;
        btnText.textContent = 'SOLD OUT';
        soldOutOverlay.classList.remove('hidden');
    } else {
        btn.disabled = false;
        btnText.textContent = 'BUY NOW (FLASH SALE)';
        soldOutOverlay.classList.add('hidden');
    }
}

// Server-Sent Events (SSE) Live Stock Streaming
function connectSSE(itemId) {
    if (eventSource) {
        eventSource.close();
    }

    eventSource = new EventSource(`/api/v1/items/${itemId}/stream`);
    eventSource.onmessage = (e) => {
        const data = JSON.parse(e.data);
        if (activeItem && data.item_id === activeItem.id) {
            updateStockDisplay(data.stock, activeItem.total_stock);
        }
    };
}

// Reserve Action
document.getElementById('reserve-btn').addEventListener('click', async () => {
    if (!activeItem) return;
    const btn = document.getElementById('reserve-btn');
    btn.disabled = true;
    const start = performance.now();

    const card = document.getElementById('status-card');
    const title = document.getElementById('status-title');
    const desc = document.getElementById('status-desc');
    const icon = document.getElementById('status-icon');

    try {
        const res = await fetch('/api/v1/orders/reserve', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`,
                'X-Idempotency-Key': `idem_${crypto.randomUUID()}`
            },
            body: JSON.stringify({ item_id: activeItem.id })
        });
        const elapsed = (performance.now() - start).toFixed(1);
        document.getElementById('telemetry-latency').textContent = `${elapsed} ms`;

        card.classList.remove('hidden');

        if (res.status === 202) {
            const data = await res.json();
            card.className = 'max-w-sm p-3.5 rounded-sm border border-emerald-300 bg-emerald-50 text-emerald-800 text-xs';
            icon.innerHTML = '<i data-lucide="check-circle-2" class="w-4 h-4 text-emerald-600"></i>';
            title.textContent = `Order Reserved! (${elapsed}ms)`;
            desc.textContent = `Stock secured in Redis! Order #${data.order_number} is processing into PostgreSQL...`;
            appendLog(`<span class="text-emerald-400 font-bold">[202 Accepted]</span> ${data.order_number} secured in ${elapsed}ms`);
            lucide.createIcons();

            pollOrderStatus(data.order_id);
        } else if (res.status === 410) {
            card.className = 'max-w-sm p-3.5 rounded-sm border border-gray-300 bg-gray-100 text-gray-700 text-xs';
            icon.innerHTML = '<i data-lucide="x-circle" class="w-4 h-4 text-gray-500"></i>';
            title.textContent = `Sold Out (${elapsed}ms)`;
            desc.textContent = 'All promotional units were claimed. Zero inventory remaining in memory.';
            appendLog(`<span class="text-gray-400 font-bold">[410 Sold Out]</span> Request rejected at edge in ${elapsed}ms`);
            lucide.createIcons();
        } else if (res.status === 409) {
            card.className = 'max-w-sm p-3.5 rounded-sm border border-amber-300 bg-amber-50 text-amber-800 text-xs';
            icon.innerHTML = '<i data-lucide="alert-triangle" class="w-4 h-4 text-amber-600"></i>';
            title.textContent = 'Purchase Limit Reached (409 Conflict)';
            desc.textContent = 'Shopee rule: Maximum 1 flash deal per user account. Switch persona above to test again.';
            appendLog(`<span class="text-yellow-400 font-bold">[409 Conflict]</span> 1-per-user violation blocked.`);
            lucide.createIcons();
        }
    } catch (err) {
        console.error("Reserve error", err);
    } finally {
        if (activeItem && activeItem.available_stock > 0) {
            btn.disabled = false;
        }
    }
});

// Order Status Polling with Jitter
async function pollOrderStatus(orderId) {
    const desc = document.getElementById('status-desc');
    for (let attempt = 1; attempt <= 10; attempt++) {
        await new Promise(r => setTimeout(r, 600 + Math.random() * 200));
        try {
            const res = await fetch(`/api/v1/orders/${orderId}`);
            if (res.status === 200) {
                const order = await res.json();
                if (order.status === 'CONFIRMED') {
                    desc.textContent = `Order #${order.order_number} confirmed! Persisted to PostgreSQL (ACID Committed).`;
                    appendLog(`<span class="text-cyan-400 font-bold">[DB Commit]</span> Order #${order.order_number} confirmed in PostgreSQL.`);
                    break;
                }
            }
        } catch (e) {}
    }
}

// Stress Test Simulation Runner
document.getElementById('bench-btn').addEventListener('click', async () => {
    const benchBtn = document.getElementById('bench-btn');
    benchBtn.disabled = true;
    appendLog(`<span class="text-yellow-400 font-bold">[Benchmark]</span> Firing 100 simultaneous concurrent shoppers...`);

    for (let i = 0; i < 100; i++) {
        document.getElementById(`dot-${i}`).className = 'h-3 bg-gray-200 rounded-xs transition-colors duration-200';
    }

    let success = 0, soldOut = 0, errors = 0;
    const promises = [];

    for (let i = 0; i < 100; i++) {
        const p = (async (index) => {
            try {
                const res = await fetch('/api/v1/orders/reserve', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${token}`,
                        'X-Correlation-ID': `bench_${index}`
                    },
                    body: JSON.stringify({ item_id: activeItem.id })
                });
                const dot = document.getElementById(`dot-${index}`);
                if (res.status === 202) {
                    dot.className = 'h-3 bg-emerald-500 rounded-xs';
                    success++;
                } else if (res.status === 410) {
                    dot.className = 'h-3 bg-gray-400 rounded-xs';
                    soldOut++;
                } else {
                    dot.className = 'h-3 bg-red-500 rounded-xs';
                    errors++;
                }
            } catch (e) {
                errors++;
            }
        })(i);
        promises.push(p);
    }

    await Promise.all(promises);
    document.getElementById('bench-summary').textContent = `Result: ${success} Secured | ${soldOut} Sold Out | ${errors} Errors`;
    appendLog(`<span class="text-emerald-400 font-bold">[Benchmark Done]</span> ${success} units claimed, ${soldOut} rejected at edge.`);
    benchBtn.disabled = false;
});

// Dropdown Listeners
document.getElementById('item-picker').addEventListener('change', (e) => {
    const found = catalog.find(x => x.id === e.target.value);
    if (found) selectItem(found);
});

document.getElementById('persona-select').addEventListener('change', () => {
    updatePersonaToken();
});

// Initialize on Load
(async () => {
    await updatePersonaToken();
    await loadCatalog();
})();
