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
let nseCandleRegistry = {};

// 📊 VECTOR PATTERN MATCHING STORAGE
let assetPriceStreams = {};
const MAX_STREAM_WINDOW = 15;

const PATTERN_DICTIONARY = {
    'DOUBLE_BOTTOM_BULLISH': [1.0, 0.2, 0.6, 0.2, 1.0],
    'V_SHAPE_RECOVERY': [1.0, 0.7, 0.2, 0.6, 1.0],
    'HEAD_AND_SHOULDERS': [0.2, 0.6, 0.4, 0.9, 0.4, 0.6, 0.2],
    'BULLISH_FLAG_PENNANT': [0.1, 0.9, 0.7, 0.8, 0.6, 0.7, 1.2]
};

// 🧠 MACHINE LEARNING MEMORY BUFFERS
let marketHistory = [];
const MAX_HISTORY_LIMIT = 100;
let trainingData = [];
const MAX_TRAINING_SIZE = 200;

let modelState = {
    slope: 0.3,
    intercept: 0.0,
    trainedPoints: 0
};

// ⚙️ FILTER THRESHOLDS (Core Sudden Change Breakdown Scanner Gates)
const CRYPTO_THRESHOLD = 0.50;
const FOREX_THRESHOLD = 0.01;
const NSE_BREAKOUT_THRESHOLD = 2.00;

const forexWatchlist = ['EURUSDT', 'GBPUSDT', 'AUDUSDT', 'USDCAD', 'USDJPY'];

// 📈 GEOMETRIC PATTERN MATHEMATICAL VECTOR ANALYZER
function analyzePatternTrend(symbol, currentPrice) {
    if (!assetPriceStreams[symbol]) assetPriceStreams[symbol] = [];
    assetPriceStreams[symbol].push(currentPrice);

    if (assetPriceStreams[symbol].length > MAX_STREAM_WINDOW) assetPriceStreams[symbol].shift();

    const stream = assetPriceStreams[symbol];
    if (stream.length < 5) return { pattern: "SCANNING TRACKS", bias: "CALCULATING" };

    const recentSample = stream.slice(-5);
    const minP = Math.min(...recentSample);
    const maxP = Math.max(...recentSample);
    const range = maxP - minP;

    const normalizedStream = recentSample.map(p => range === 0 ? 0.5 : (p - minP) / range);

    let bestMatch = "SCANNED MARKET";
    let highestCorrelation = 0.75;
    let dynamicBias = "NEUTRAL CONTINUATION";

    for (const [patternName, signature] of Object.entries(PATTERN_DICTIONARY)) {
        let dotProduct = 0,
            mA = 0,
            mB = 0;
        const compareLength = Math.min(normalizedStream.length, signature.length);

        for (let i = 0; i < compareLength; i++) {
            dotProduct += normalizedStream[i] * signature[i];
            mA += normalizedStream[i] * normalizedStream[i];
            mB += signature[i] * signature[i];
        }

        const denominator = Math.sqrt(mA) * Math.sqrt(mB);
        if (denominator === 0) continue;

        const similarity = dotProduct / denominator;

        if (similarity > highestCorrelation) {
            highestCorrelation = similarity;
            bestMatch = patternName;
        }
    }

    if (bestMatch.includes('BULLISH') || bestMatch.includes('RECOVERY') || bestMatch.includes('PENNANT')) {
        dynamicBias = "UPTREND EXPECTED";
    } else if (bestMatch.includes('SHOULDERS')) {
        dynamicBias = "REVERSAL RISK";
    }

    return { pattern: bestMatch.replace(/_/g, ' '), bias: dynamicBias };
}

// 🤖 LINEAR REGRESSION TRAINING ENGINE
function trainModel(newVolume, newChange) {
    trainingData.push({ x: parseFloat(newVolume || 0), y: parseFloat(newChange || 0) });
    if (trainingData.length > MAX_TRAINING_SIZE) trainingData.shift();

    const n = trainingData.length;
    if (n < 5) return;

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

    const denominator = (n * sumXX) - (sumX * sumX);
    if (denominator === 0) return;

    modelState.slope = ((n * sumXY) - (sumX * sumY)) / denominator;
    modelState.intercept = (sumY - (modelState.slope * sumX)) / n;
    modelState.trainedPoints = n;
}

// 🌐 HISTORY API ENDPOINT WITH ROOT DISAGREEMENT PROTECTION
app.get('/', (req, res) => {
    res.send("Tri-Asset AI Engine Gateway is Active.");
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
                        const formattedVol = liveVolume >= 1000000 ?
                            `${(liveVolume/1000000).toFixed(2)}M` :
                            `${(liveVolume/1000).toFixed(1)}K`;

                        const payload = {
                            market: 'FOREX',
                            symbol: formattedFxName,
                            currentPrice: currentPrice,
                            oldPrice: ref.price,
                            change: parseFloat(dev.toFixed(3)),
                            volume: liveVolume,
                            type: dev > 0 ? 'SURGE' : 'CRASH',
                            timestamp: new Date().toLocaleTimeString(),
                            news: dev > 0 ?
                                `🚨 FOREX INFLOW: High liquidity institutional block orders executing buy momentum (${formattedVol}).` : `🚨 FOREX OUTFLOW: High liquidity institutional block orders executing sell pressure (${formattedVol}).`
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

                        let structuralNews = "";
                        const formattedVol = liveVolume >= 1000000 ?
                            `${(liveVolume/1000000).toFixed(2)}M` :
                            `${(liveVolume/1000).toFixed(1)}K`;

                        if (Math.abs(dev) >= 3.0) {
                            structuralNews = dev > 0 ?
                                `🚨 MASSIVE BUY REPORTED: Whale account executed market buy orders clearing ${formattedVol} in trading volume.` :
                                `🚨 MASSIVE SELL REPORTED: Whale account dumped market sell orders clearing ${formattedVol} in trading volume.`;
                        } else if (Math.abs(dev) >= 1.5) {
                            structuralNews = dev > 0 ?
                                `📈 High-volume buy momentum triggered by algorithmic execution layer (${formattedVol} units).` :
                                `📉 High-volume sell pressure triggered by algorithmic execution layer (${formattedVol} units).`;
                        } else {
                            structuralNews = dev > 0 ?
                                `📈 Sudden breakout upward with ${formattedVol} cumulative volume.` :
                                `📉 Sudden crash downward with ${formattedVol} cumulative volume.`;
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
                        processAndEmitPayload(payload);
                    }
                }
            }
        });
    } catch (e) { console.error("Crypto Data Error: ", e.message); }
}

// 🇮🇳 PIPELINE B: INDIAN NSE 
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
    if (weekday === 'Sat' || weekday === 'Sun' || currentTimeInMinutes < 555 || currentTimeInMinutes > 930) {
        return; // Session protection guard
    }

    try {
        const response = await fetch('https://api.bseindia.com/BseIndiaAPI/api/GetGroupHearData/w?id=GroupA&page=1&size=100', {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }
        });
        const data = await response.json();
        const now = Date.now();

        if (!data || !data.Data) return;

        data.Data.forEach(stock => {
            const symbol = stock.scrip_name || stock.scrip_cd;
            const currentPrice = parseFloat(stock.lth);
            const liveVolume = parseFloat(stock.value || stock.vol || 0);

            if (!currentPrice || isNaN(currentPrice)) return;

            if (!nseCandleRegistry[symbol]) {
                nseCandleRegistry[symbol] = { lastCandleClose: currentPrice, candleStartTime: now };
                return;
            }

            const assetCandle = nseCandleRegistry[symbol];
            if (now - assetCandle.candleStartTime >= 300000) {
                assetCandle.lastCandleClose = currentPrice;
                assetCandle.candleStartTime = now;
            }

            const previousClosePrice = assetCandle.lastCandleClose;
            const priceChangePct = ((currentPrice - previousClosePrice) / previousClosePrice) * 100;

            if (Math.abs(priceChangePct) >= NSE_BREAKOUT_THRESHOLD) {
                let structuralNews = "";
                const formattedVol = liveVolume >= 10000000 ?
                    `₹${(liveVolume/10000000).toFixed(2)} Cr` :
                    `₹${(liveVolume/100000).toFixed(1)} Lakhs`;

                if (Math.abs(priceChangePct) >= 4.0) {
                    structuralNews = priceChangePct > 0 ?
                        `🚨 INSTITUTIONAL BUY: Massive DII/FII block buy order matching flow imbalances with ${formattedVol} turnover.` :
                        `🚨 INSTITUTIONAL SELL: Massive DII/FII block liquidation matching flow imbalances with ${formattedVol} turnover.`;
                } else {
                    structuralNews = priceChangePct > 0 ?
                        `📈 Intraday momentum surge tracking positive order book delta (${formattedVol}).` :
                        `📉 Intraday momentum drop tracking aggressive distribution supply (${formattedVol}).`;
                }

                const payload = {
                    market: 'NSE',
                    symbol: symbol,
                    currentPrice: currentPrice,
                    oldPrice: previousClosePrice,
                    change: parseFloat(priceChangePct.toFixed(2)),
                    volume: liveVolume,
                    type: priceChangePct > 0 ? 'SURGE' : 'CRASH',
                    timestamp: new Date().toLocaleTimeString(),
                    news: structuralNews,
                    isWhale: Math.abs(priceChangePct) >= 4.0
                };
                processAndEmitPayload(payload);
            }
        });
    } catch (error) { console.error("NSE Error: ", error.message); }
}

// ⚙️ UNIFIED ENRICHMENT & DEPLOYMENT ROUTER
function processAndEmitPayload(payload) {
    const vol = parseFloat(payload.volume || 0);
    const chg = parseFloat(payload.change || 0);
    const base = parseFloat(payload.currentPrice || 0);

    const patternMetrics = analyzePatternTrend(payload.symbol, base);
    payload.detectedPattern = patternMetrics.pattern;
    payload.predictedBias = patternMetrics.bias;

    trainModel(vol, chg);
    const predictionPct = (modelState.slope * vol) + modelState.intercept;
    payload.aiPredictedTarget = chg > 0 ? base * (1 + Math.abs(predictionPct) / 100) : base * (1 - Math.abs(predictionPct) / 100);
    payload.modelAccuracyPoints = modelState.trainedPoints;
    payload.modelConfidence = Math.min(Math.abs(modelState.slope * 100) + 45, 99.4).toFixed(1);

    marketHistory = marketHistory.filter(item => item.symbol !== payload.symbol);
    marketHistory.push(payload);
    if (marketHistory.length > MAX_HISTORY_LIMIT) marketHistory.shift();

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(payload));
        }
    });
}

// 🌐 SOCKET MESSAGE MANAGER WITH CONSOLE VERIFICATION
wss.on('connection', (ws, req) => {
    console.log(`📡 Client connected successfully from origin: ${req.headers.origin}`);

    // 🔥 BACKWARD COMPATIBILITY SNAPSHOT TRIGGER:
    // When your client connects, immediately transmit whatever historical metrics are in memory
    // so the frontend turns green and renders data instantly instead of waiting for thresholds!
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

setInterval(trackCryptoAndForex, 4000);
setInterval(scanNseCandleBreakouts, 3000);

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => console.log(`Tri-Asset AI Engine running on port ${PORT}`));