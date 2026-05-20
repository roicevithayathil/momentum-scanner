const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'] }));
app.set('trust proxy', 1);

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let marketHistory = [];
const MAX_HISTORY_LIMIT = 100;

// Filter thresholds
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_THRESHOLD = 0.15; // Emits precise, volatile momentum changes

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];
// Target list of core Indian market heavyweights across sectors
const nseSymbols = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS', 'TATAMOTORS.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'ITC.NS', 'LT.NS'];

app.get('/', (req, res) => res.send("Tri-Asset Engine Gateway Active."));
app.get('/api/history', (req, res) => res.json(marketHistory));

// 🪙 PIPELINE A: CRYPTO & FOREX (Binance Gateway)
async function trackCryptoAndForex() {
    try {
        const response = await fetch('https://api.binance.com/api/v3/ticker/24hr');
        const tickers = await response.json();
        if (!Array.isArray(tickers)) return;

        tickers.forEach(ticker => {
            const symbol = ticker.symbol;
            const currentPrice = parseFloat(ticker.lastPrice || ticker.price);
            const liveVolume = parseFloat(ticker.volume || 0);

            if (forexWatchlist.includes(symbol)) {
                const formattedFxName = symbol.slice(0, 3) + '/' + symbol.slice(3, 6);
                if (!global.fxHist) global.fxHist = {};
                const oldPrice = global.fxHist[formattedFxName] || currentPrice;
                const dev = oldPrice !== currentPrice ? ((currentPrice - oldPrice) / oldPrice) * 100 : 0;

                if (Math.abs(dev) >= FOREX_THRESHOLD) {
                    global.fxHist[formattedFxName] = currentPrice;
                    processAndEmitPayload({
                        market: 'FOREX',
                        symbol: formattedFxName,
                        currentPrice,
                        oldPrice,
                        change: parseFloat(dev.toFixed(3)),
                        volume: liveVolume,
                        type: dev > 0 ? 'SURGE' : 'CRASH',
                        timestamp: new Date().toLocaleTimeString(),
                        news: `💧 FOREX: Institutional block variance shift at ${currentPrice}.`
                    });
                }
                return;
            }

            if (symbol.endsWith('USDT')) {
                if (!global.cryHist) global.cryHist = {};
                const oldPrice = global.cryHist[symbol] || currentPrice;
                const dev = oldPrice !== currentPrice ? ((currentPrice - oldPrice) / oldPrice) * 100 : 0;

                if (Math.abs(dev) >= CRYPTO_THRESHOLD) {
                    global.cryHist[symbol] = currentPrice;
                    processAndEmitPayload({
                        market: 'CRYPTO',
                        symbol,
                        currentPrice,
                        oldPrice,
                        change: parseFloat(dev.toFixed(2)),
                        volume: liveVolume,
                        type: dev > 0 ? 'SURGE' : 'CRASH',
                        timestamp: new Date().toLocaleTimeString(),
                        news: `🪙 CRYPTO: Volatility momentum surge crossing standard deviation bands.`
                    });
                }
            }
        });
    } catch (e) { console.error("Crypto Pipe Error: ", e.message); }
}

// 🇮🇳 PIPELINE B: FREE LIVE NSE ENGINE
async function streamRealNseData() {
    try {
        // Fetching directly from the reliable global open-source stock router proxy
        for (const fullTicker of nseSymbols) {
            const response = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${fullTicker}?interval=1m&range=1d`, {
                headers: { 'User-Agent': 'Mozilla/5.0' }
            });
            const data = await response.json();

            const meta = data ? .chart ? .result ? .[0] ? .meta;
            if (!meta) continue;

            const symbol = meta.symbol.replace('.NS', '');
            const currentPrice = meta.regularMarketPrice;
            const previousClose = meta.previousClose;
            const liveVolume = meta.regularMarketVolume || 0;
            const priceChangePct = ((currentPrice - previousClose) / previousClose) * 100;

            if (Math.abs(priceChangePct) >= NSE_THRESHOLD) {
                const formattedVol = liveVolume >= 1000000 ? `₹${(liveVolume/1000000).toFixed(2)}M Vol` : `${liveVolume} Shares`;

                processAndEmitPayload({
                    market: 'NSE',
                    symbol: symbol,
                    currentPrice: parseFloat(currentPrice.toFixed(2)),
                    oldPrice: parseFloat(previousClose.toFixed(2)),
                    change: parseFloat(priceChangePct.toFixed(2)),
                    volume: liveVolume,
                    type: priceChangePct > 0 ? 'SURGE' : 'CRASH',
                    timestamp: new Date().toLocaleTimeString(),
                    news: priceChangePct > 0 ?
                        `📈 Real-time buying pressure acceleration noted for ${symbol} (${formattedVol}).` :
                        `📉 Distribution liquidity outflow hitting ${symbol} order books.`
                });
            }
        }
    } catch (error) { console.error("NSE Live Proxy Error: ", error.message); }
}

function processAndEmitPayload(payload) {
    marketHistory = marketHistory.filter(item => item.symbol !== payload.symbol);
    marketHistory.push(payload);
    if (marketHistory.length > MAX_HISTORY_LIMIT) marketHistory.shift();

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(payload));
    });
}

wss.on('connection', (ws) => {
    if (marketHistory.length > 0) {
        marketHistory.forEach(cachedPayload => {
            if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(cachedPayload));
        });
    }
});

// Run tracking workers
setInterval(trackCryptoAndForex, 4000);
setInterval(streamRealNseData, 5000); // Polling window adjusted to maintain rapid requests safely

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`Tri-Asset Engine active on port ${PORT}`));