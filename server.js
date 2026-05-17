const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Tracking registers for candle architecture
let cryptoHistory = {};
let forexHistory = {};
let nseCandleRegistry = {};

// ⚙️ TRADING FILTERS & PRO-PARAMETERS
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_BREAKOUT_THRESHOLD = 2.00; // 🎯 STSTRICT 2% MOMENTUM GAIN/LOSS GATE FOR INTRADAY

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];

// 🪙 PIPELINE A: CRYPTO & FOREX DESK (Runs 24/7)
async function trackCryptoAndForex() {
    try {
        const response = await fetch('https://api.binance.com/api/v3/ticker/price');
        const tickers = await response.json();
        const now = Date.now();

        tickers.forEach(ticker => {
            const symbol = ticker.symbol;
            const currentPrice = parseFloat(ticker.price);

            if (forexWatchlist.includes(symbol)) {
                const formattedFxName = symbol.slice(0, 3) + '/' + symbol.slice(3, 6);
                if (!forexHistory[formattedFxName]) forexHistory[formattedFxName] = [];
                forexHistory[formattedFxName].push({ timestamp: now, price: currentPrice });
                forexHistory[formattedFxName] = forexHistory[formattedFxName].filter(p => now - p.timestamp <= 300000);

                const ref = forexHistory[formattedFxName][0];
                if (ref && ref.price !== currentPrice) {
                    const dev = ((currentPrice - ref.price) / ref.price) * 100;
                    if (Math.abs(dev) >= FOREX_THRESHOLD) {
                        broadcast({
                            market: 'FOREX',
                            symbol: formattedFxName,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: dev.toFixed(3),
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString()
                        });
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
                        broadcast({
                            market: 'CRYPTO',
                            symbol: symbol,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: dev.toFixed(2),
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString()
                        });
                    }
                }
            }
        });
    } catch (e) { console.error(e.message); }
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
        // Fetch snapshot of liquid high-volume stocks from public index endpoints
        const response = await fetch('https://api.bseindia.com/BseIndiaAPI/api/GetGroupHearData/w?id=GroupA&page=1&size=100');
        const data = await response.json();
        const now = Date.now();

        if (!data || !data.Data) return;

        data.Data.forEach(stock => {
            const symbol = stock.scrip_cd;
            const currentPrice = parseFloat(stock.lth); // Last Traded Price (LTP)

            if (!currentPrice || isNaN(currentPrice)) return;

            // Initialize or establish candle closure baselines
            if (!nseCandleRegistry[symbol]) {
                nseCandleRegistry[symbol] = {
                    lastCandleClose: currentPrice,
                    candleStartTime: now
                };
                return;
            }

            const assetCandle = nseCandleRegistry[symbol];

            // ⏱️ CANDLE ARCHITECTURE CLOCK TIMER:
            // Check if 5 minutes (300,000ms) have passed to roll over and lock in the candle close
            if (now - assetCandle.candleStartTime >= 300000) {
                assetCandle.lastCandleClose = currentPrice; // Set current price as the new baseline close
                assetCandle.candleStartTime = now; // Reset the timer for the next candle
                console.log(`🔒 [CANDLE ROLLOVER] Locked new 5M candle close for ${symbol} at ₹${currentPrice}`);
            }

            // 🧮 MOMENTUM ENGINE MATH: Compare real-time price against the fixed candle close
            const previousClosePrice = assetCandle.lastCandleClose;
            const priceChangePct = ((currentPrice - previousClosePrice) / previousClosePrice) * 100;

            // 🚫 THE 2% RADAR GATE: Completely ignores anything moving less than 2%
            if (Math.abs(priceChangePct) >= NSE_BREAKOUT_THRESHOLD) {
                const triggerType = priceChangePct > 0 ? 'SURGE' : 'CRASH';

                console.log(`🎯 BREAKOUT: ${symbol} moved ${priceChangePct.toFixed(2)}% from previous candle close!`);

                broadcast({
                    market: 'NSE',
                    symbol: symbol,
                    currentPrice: currentPrice,
                    oldPrice: previousClosePrice,
                    change: priceChangePct.toFixed(2),
                    type: triggerType,
                    timestamp: new Date().toLocaleTimeString()
                });
            }
        });
    } catch (error) {
        console.error("NSE Data Pipeline Scraper Error: ", error.message);
    }
}

function broadcast(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(data));
    });
}

// Global Execution Interval Handlers
setInterval(trackCryptoAndForex, 4000);
setInterval(scanNseCandleBreakouts, 3000); // Scrapes the index snapshots every 3 seconds

// Force the engine to listen to Render's dynamic port environment or fallback to 5000
const PORT = process.env.PORT || 5000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Institutional Tri-Asset Engine online on port ${PORT}`);
});