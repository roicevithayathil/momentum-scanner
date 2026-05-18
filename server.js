const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();

// 🔓 Robust, aggressive CORS handling to guarantee GitHub Pages can connect
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));

app.set('trust proxy', 1); // Allow Render's reverse proxy to route WebSockets cleanly

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// 🧠 TRACKING REGISTERS FOR REAL-TIME PIPELINES
let cryptoHistory = {};
let forexHistory = {};
let marketHistory = [];
const MAX_HISTORY_LIMIT = 100;

// ⚙️ FILTER THRESHOLDS (Optimized to guarantee constant live data stream)
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_BREAKOUT_THRESHOLD = 0.10; // Dynamic tracking for realistic equity shifts

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];

// 🌐 HISTORY API ENDPOINT
app.get('/', (req, res) => {
    res.send("Tri-Asset Engine Gateway is Active.");
});

app.get('/api/history', (req, res) => {
    res.json(marketHistory);
});

// 🪙 PIPELINE A: CRYPTO & FOREX 
async function trackCryptoAndForex() {
    try {
        const response = await fetch('https://api.binance.com/api/v3/ticker/24hr', {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
        });
        const tickers = await response.json();
        const now = Date.now();

        if (!Array.isArray(tickers)) return;

        tickers.forEach(ticker => {
            const symbol = ticker.symbol;
            const currentPrice = parseFloat(ticker.lastPrice || ticker.price);
            const liveVolume = parseFloat(ticker.volume || 0);

            if (forexWatchlist.includes(symbol)) {
                const formattedFxName = symbol.slice(0, 3) + '/' + symbol.slice(3, 6);
                if (!forexHistory[formattedFxName]) forexHistory[formattedFxName] = [];
                forexHistory[formattedFxName].push({ timestamp: now, price: currentPrice });
                forexHistory[formattedFxName] = forexHistory[formattedFxName].filter(p => now - p.timestamp <= 300000);

                const ref = forexHistory[formattedFxName][0];
                if (ref && ref.price !== currentPrice) {
                    const dev = ((currentPrice - ref.price) / ref.price) * 100;
                    if (Math.abs(dev) >= FOREX_THRESHOLD) {
                        const formattedVol = liveVolume >= 1000000 ? `${(liveVolume/1000000).toFixed(2)}M` : `${(liveVolume/1000).toFixed(1)}K`;

                        const payload = {
                            market: 'FOREX',
                            symbol: formattedFxName,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: parseFloat(dev.toFixed(3)),
                            volume: liveVolume,
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: dev > 0 ? `🚨 FOREX INFLOW: Institutional block orders executing buy momentum (${formattedVol}).` : `🚨 FOREX OUTFLOW: Institutional block orders executing sell pressure (${formattedVol}).`
                        };
                        processAndEmitPayload(payload);
                    }
                }
                return;
            }

            if (symbol.endsWith('USDT')) {
                if (!cryptoHistory[symbol]) cryptoHistory[symbol] = [];
                cryptoHistory[symbol].push({ timestamp: now, price: currentPrice });
                cryptoHistory[symbol] = cryptoHistory[symbol].filter(p => now - p.timestamp <= 300000);

                const ref = cryptoHistory[symbol][0];
                if (ref && ref.price !== currentPrice) {
                    const dev = ((currentPrice - ref.price) / ref.price) * 100;
                    if (Math.abs(dev) >= CRYPTO_THRESHOLD) {
                        const formattedVol = liveVolume >= 1000000 ? `${(liveVolume/1000000).toFixed(2)}M` : `${(liveVolume/1000).toFixed(1)}K`;

                        const payload = {
                            market: 'CRYPTO',
                            symbol: symbol,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: parseFloat(dev.toFixed(2)),
                            volume: liveVolume,
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: dev > 0 ? `📈 Breakout upward with ${formattedVol} cumulative volume.` : `📉 Sudden crash downward with ${formattedVol} cumulative volume.`
                        };
                        processAndEmitPayload(payload);
                    }
                }
            }
        });
    } catch (e) { console.error("Crypto Data Error: ", e.message); }
}

// 🇮🇳 PIPELINE B: INDIAN NSE (Optimized Intraday Baseline)
async function scanNseCandleBreakouts() {
    try {
        const response = await fetch('https://api.bseindia.com/BseIndiaAPI/api/GetGroupHearData/w?id=GroupA&page=1&size=100', {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
        });
        const data = await response.json();

        if (!data || !data.Data) return;

        data.Data.forEach(stock => {
            const symbol = stock.scrip_name || stock.scrip_cd;
            const currentPrice = parseFloat(stock.lth); // Last Traded High/Price
            const liveVolume = parseFloat(stock.value || stock.vol || 0);

            // Extract the baseline day open or previous close directly from the API object safely
            const baselinePrice = parseFloat(stock.prev_close || stock.open || currentPrice);

            if (!currentPrice || isNaN(currentPrice) || !baselinePrice) return;

            // Calculate exact intraday performance variance
            const priceChangePct = ((currentPrice - baselinePrice) / baselinePrice) * 100;

            // Using standard thresholding to catch active, liquid stock changes instantly
            if (Math.abs(priceChangePct) >= NSE_BREAKOUT_THRESHOLD) {
                const formattedVol = liveVolume >= 10000000 ? `₹${(liveVolume/10000000).toFixed(2)} Cr` : `₹${(liveVolume/100000).toFixed(1)} Lakhs`;

                const payload = {
                    market: 'NSE',
                    symbol: symbol.trim(),
                    currentPrice: currentPrice,
                    oldPrice: baselinePrice,
                    change: parseFloat(priceChangePct.toFixed(2)),
                    volume: liveVolume,
                    type: priceChangePct > 0 ? 'SURGE' : 'CRASH',
                    timestamp: new Date().toLocaleTimeString(),
                    news: priceChangePct > 0 ? `📈 Intraday momentum surge tracking positive order book delta (${formattedVol}).` : `📉 Intraday momentum drop tracking aggressive distribution supply (${formattedVol}).`
                };
                processAndEmitPayload(payload);
            }
        });
    } catch (error) { console.error("NSE Error: ", error.message); }
}

// ⚙️ UNIFIED EMIT ROUTER
function processAndEmitPayload(payload) {
    marketHistory = marketHistory.filter(item => item.symbol !== payload.symbol);
    marketHistory.push(payload);
    if (marketHistory.length > MAX_HISTORY_LIMIT) marketHistory.shift();

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(payload));
        }
    });
}

// 🌐 SOCKET MESSAGE MANAGER
wss.on('connection', (ws, req) => {
    console.log(`📡 Client connected from origin: ${req.headers.origin}`);

    if (marketHistory.length > 0) {
        marketHistory.forEach(cachedPayload => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(cachedPayload));
            }
        });
    }

    ws.on('message', (msg) => {
        try {
            const parsed = JSON.parse(msg);
            if (parsed.type === 'PING') ws.send(JSON.stringify({ type: 'PONG' }));
        } catch (e) {}
    });
});

// Run pipelines immediately and establish tracking loops
trackCryptoAndForex();
scanNseCandleBreakouts();

setInterval(trackCryptoAndForex, 4000);
setInterval(scanNseCandleBreakouts, 4000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`Tri-Asset Engine running on port ${PORT}`));