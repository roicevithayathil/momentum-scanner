const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors()); // 🔓 Allows GitHub Pages frontend to pull historical data safely

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Tracking registers for candle architecture
let cryptoHistory = {};
let forexHistory = {};
let nseCandleRegistry = {};

// 🧠 NEW: HISTORICAL MEMORY BUFFER (Stores last 100 global structural breakout payloads)
let marketHistory = [];
const MAX_HISTORY_LIMIT = 100;

// ⚙️ TRADING FILTERS & PRO-PARAMETERS
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_BREAKOUT_THRESHOLD = 2.00; // 🎯 STRICT 2% MOMENTUM GAIN/LOSS GATE FOR INTRADAY

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];

// 🌐 NEW: HISTORY REST ENDPOINT (Feeds the frontend instantly when the webpage opens)
app.get('/api/history', (req, res) => {
    res.json(marketHistory);
});

// 🪙 PIPELINE A: CRYPTO & FOREX DESK (Upgraded to pull live 24h traded volumes)
async function trackCryptoAndForex() {
    try {
        // 🔄 Optimized to /ticker/24hr to capture real-time volume metrics alongside raw price data
        const response = await fetch('https://api.binance.com/api/v3/ticker/24hr');
        const tickers = await response.json();
        const now = Date.now();

        tickers.forEach(ticker => {
            const symbol = ticker.symbol;
            const currentPrice = parseFloat(ticker.lastPrice || ticker.price);
            const liveVolume = parseFloat(ticker.volume || 0); // Real-time trading volume

            if (forexWatchlist.includes(symbol)) {
                const formattedFxName = symbol.slice(0, 3) + '/' + symbol.slice(3, 6);
                if (!forexHistory[formattedFxName]) forexHistory[formattedFxName] = [];
                forexHistory[formattedFxName].push({ timestamp: now, price: currentPrice });
                forexHistory[formattedFxName] = forexHistory[formattedFxName].filter(p => now - p.timestamp <= 300000);

                const ref = forexHistory[formattedFxName][0];
                if (ref && ref.price !== currentPrice) {
                    const dev = ((currentPrice - ref.price) / ref.price) * 100;
                    if (Math.abs(dev) >= FOREX_THRESHOLD) {
                        const payload = {
                            market: 'FOREX',
                            symbol: formattedFxName,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: parseFloat(dev.toFixed(3)),
                            volume: liveVolume,
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: Math.abs(dev) > 0.05 ? "Institutional volume block order execution clearing." : ""
                        };

                        // Save into backend memory cache and broadcast
                        saveToHistoryCache(payload);
                        broadcast(payload);
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

                        // Contextual News Triggering Engine
                        let structuralNews = "";
                        if (Math.abs(dev) >= 3.0) {
                            structuralNews = "Whale wallet cluster aggregation pushing orderbook liquidity bounds.";
                        } else if (Math.abs(dev) >= 1.5) {
                            structuralNews = "High-frequency algorithmic trend-following momentum execution.";
                        }

                        const payload = {
                            market: 'CRYPTO',
                            symbol: symbol,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: parseFloat(dev.toFixed(2)),
                            volume: liveVolume, // Real-time volume integrated
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: structuralNews,
                            isWhale: Math.abs(dev) >= 3.0 // Flag large movements as whales
                        };

                        saveToHistoryCache(payload);
                        broadcast(payload);
                    }
                }
            }
        });
    } catch (e) { console.error("Crypto/Forex Data Pipeline Error: ", e.message); }
}

// 🇮🇳 PIPELINE B: INSTITUTIONAL NSE REAL-TIME SQUEEZE DETECTOR
async function scanNseCandleBreakouts() {
    // ⏰ Live Session Clock Guard
    const options = { timeZone: 'Asia/Kolkata', hour12: false, weekday: 'short', hour: '2-digit', minute: '2-digit' };
    const indiaTimeFormatter = new Intl.DateTimeFormat('en-US', options);
    const parts = indiaTimeFormatter.formatToParts(new Date());

    let weekday = '',
        hour = 0,
        minute = 0;
    parts.forEach(p => {
        if (p.type === 'weekday') weekday = p.value;
        if (p.type === 'hour') hour = parseInt(p.value, 10);
        if (p.type === 'minute') minute = parseInt(p.value, 10);
    });

    const currentTimeInMinutes = (hour * 60) + minute;
    const marketOpenMinutes = (9 * 60) + 15;
    const marketCloseMinutes = (15 * 60) + 30;

    if (weekday === 'Sat' || weekday === 'Sun' || currentTimeInMinutes < marketOpenMinutes || currentTimeInMinutes > marketCloseMinutes) {
        return;
    }

    try {
        const response = await fetch('https://api.bseindia.com/BseIndiaAPI/api/GetGroupHearData/w?id=GroupA&page=1&size=100');
        const data = await response.json();
        const now = Date.now();

        if (!data || !data.Data) return;

        data.Data.forEach(stock => {
            const symbol = stock.scrip_name || stock.scrip_cd;
            const currentPrice = parseFloat(stock.lth); // Last Traded Price (LTP)
            const liveVolume = parseFloat(stock.value || stock.vol || 0); // Capture active dollar/rupee volume turnover

            if (!currentPrice || isNaN(currentPrice)) return;

            if (!nseCandleRegistry[symbol]) {
                nseCandleRegistry[symbol] = {
                    lastCandleClose: currentPrice,
                    candleStartTime: now
                };
                return;
            }

            const assetCandle = nseCandleRegistry[symbol];

            if (now - assetCandle.candleStartTime >= 300000) {
                assetCandle.lastCandleClose = currentPrice;
                assetCandle.candleStartTime = now;
                console.log(`🔒 [CANDLE ROLLOVER] Locked new 5M candle close for ${symbol} at ₹${currentPrice}`);
            }

            const previousClosePrice = assetCandle.lastCandleClose;
            const priceChangePct = ((currentPrice - previousClosePrice) / previousClosePrice) * 100;

            if (Math.abs(priceChangePct) >= NSE_BREAKOUT_THRESHOLD) {
                const triggerType = priceChangePct > 0 ? 'SURGE' : 'CRASH';

                let structuralNews = "";
                if (Math.abs(priceChangePct) >= 4.0) {
                    structuralNews = "Institutional DII/FII block volume surge matching massive order flow imbalances.";
                }

                const payload = {
                    market: 'NSE',
                    symbol: symbol,
                    currentPrice: currentPrice,
                    oldPrice: previousClosePrice,
                    change: parseFloat(priceChangePct.toFixed(2)),
                    volume: liveVolume,
                    type: triggerType,
                    timestamp: new Date().toLocaleTimeString(),
                    news: structuralNews,
                    isWhale: Math.abs(priceChangePct) >= 4.0
                };

                console.log(`🎯 BREAKOUT: ${symbol} moved ${priceChangePct.toFixed(2)}% with volume!`);

                saveToHistoryCache(payload);
                broadcast(payload);
            }
        });
    } catch (error) {
        console.error("NSE Data Pipeline Scraper Error: ", error.message);
    }
}

// 🧠 AUXILIARY CORE: COMMITS DATA ROUTINELY INTO RUNNING HISTORY MATRIX
function saveToHistoryCache(payload) {
    // Avoid appending exact duplicate records of the same ticker to keep the cache clean
    marketHistory = marketHistory.filter(item => item.symbol !== payload.symbol);

    marketHistory.push(payload);

    // Hard limit to save memory overhead on free cloud tiers
    if (marketHistory.length > MAX_HISTORY_LIMIT) {
        marketHistory.shift();
    }
}

function broadcast(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(data));
    });
}

// Global Execution Interval Handlers
setInterval(trackCryptoAndForex, 4000);
setInterval(scanNseCandleBreakouts, 3000);

const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Institutional Tri-Asset Engine online on port ${PORT}`);
});