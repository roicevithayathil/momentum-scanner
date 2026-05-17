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

// 🧠 HISTORICAL MEMORY BUFFER (Stores last 100 global structural breakout payloads)
let marketHistory = [];
const MAX_HISTORY_LIMIT = 100;

// Rolling training memory buffer for the learning model (tracks last 200 high-vol events)
let trainingData = [];
const MAX_TRAINING_SIZE = 200;

// Default fallback coefficients before the model learns from live data
let modelState = {
    slope: 0.3,
    intercept: 0.0,
    trainedPoints: 0
};

// ⚙️ TRADING FILTERS & PRO-PARAMETERS
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_BREAKOUT_THRESHOLD = 2.00; // 🎯 STRICT 2% MOMENTUM GAIN/LOSS GATE FOR INTRADAY

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];

// Train the Linear Regression model on-the-fly using ordinary least squares
function trainModel(newVolume, newChange) {
    // Save normalized metrics to training memory
    trainingData.push({ x: parseFloat(newVolume || 0), y: parseFloat(newChange || 0) });
    if (trainingData.length > MAX_TRAINING_SIZE) trainingData.shift();

    const n = trainingData.length;
    if (n < 5) return; // Wait for at least 5 baseline points to start learning

    let sumX = 0,
        sumY = 0,
        sumXY = 0,
        sumXX = 0;
    for (let i = 0; i < n; i++) {
        sumX += trainingData[i].x;
        sumY += trainingData[i].y;
        sumXY += (trainingData[i].x * trainingData[i].y);
        sumXX += (trainingData[i].x * trainingData[i].x);
    }

    // Linear Regression Formula calculation
    const denominator = (n * sumXX) - (sumX * sumX);
    if (denominator === 0) return; // Prevent division by zero

    modelState.slope = ((n * sumXY) - (sumX * sumY)) / denominator;
    modelState.intercept = (sumY - (modelState.slope * sumX)) / n;
    modelState.trainedPoints = n;

    console.log(`[🤖 AI MODEL UPDATED] Trained Points: ${n} | Slope: ${modelState.slope.toFixed(6)} | Intercept: ${modelState.intercept.toFixed(4)}`);
}

// 🌐 HISTORY REST ENDPOINT (Feeds the frontend instantly when the webpage opens)
app.get('/api/history', (req, res) => {
    res.json(marketHistory);
});

// 🪙 PIPELINE A: CRYPTO & FOREX DESK (Upgraded to pull live 24h traded volumes)
async function trackCryptoAndForex() {
    try {
        const response = await fetch('https://api.binance.com/api/v3/ticker/24hr');
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
                            volume: liveVolume,
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: structuralNews,
                            isWhale: Math.abs(dev) >= 3.0
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
            const currentPrice = parseFloat(stock.lth);
            const liveVolume = parseFloat(stock.value || stock.vol || 0);

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
    marketHistory = marketHistory.filter(item => item.symbol !== payload.symbol);

    // Inject machine learning metrics directly into cached items
    const volumeMetric = parseFloat(payload.volume || 0);
    const changeMetric = parseFloat(payload.change || 0);

    trainModel(volumeMetric, changeMetric);

    const basePrice = parseFloat(payload.currentPrice || 0);
    const predictedChangePct = (modelState.slope * volumeMetric) + modelState.intercept;

    payload.aiPredictedTarget = changeMetric > 0 ?
        basePrice * (1 + Math.abs(predictedChangePct) / 100) :
        basePrice * (1 - Math.abs(predictedChangePct) / 100);

    payload.modelAccuracyPoints = modelState.trainedPoints;
    payload.modelConfidence = Math.min(Math.abs(modelState.slope * 100) + 40, 99.2).toFixed(1);

    marketHistory.push(payload);
    if (marketHistory.length > MAX_HISTORY_LIMIT) {
        marketHistory.shift();
    }
}

function broadcast(data) {
    // Inject ML metrics on active live broadcasts
    const volumeMetric = parseFloat(data.volume || 0);
    const changeMetric = parseFloat(data.change || 0);

    const basePrice = parseFloat(data.currentPrice || 0);
    const predictedChangePct = (modelState.slope * volumeMetric) + modelState.intercept;

    data.aiPredictedTarget = changeMetric > 0 ?
        basePrice * (1 + Math.abs(predictedChangePct) / 100) :
        basePrice * (1 - Math.abs(predictedChangePct) / 100);

    data.modelAccuracyPoints = modelState.trainedPoints;
    data.modelConfidence = Math.min(Math.abs(modelState.slope * 100) + 40, 99.2).toFixed(1);

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

// 🌐 Handle incoming Frontend Client messages (like the Ping Heartbeat Loop)
wss.on('connection', (ws) => {
    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            if (parsed.type === 'PING') {
                ws.send(JSON.stringify({ type: 'PONG' }));
            }
        } catch (e) {}
    });
});

// Global Execution Interval Handlers
setInterval(trackCryptoAndForex, 4000);
setInterval(scanNseCandleBreakouts, 3000);

// Dynamic port configuration with clean network routing
const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`Institutional Tri-Asset Engine online on port ${PORT}`);
});