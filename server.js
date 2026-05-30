const express = require('express');
const cors = require('cors');
const axios = require('axios');
const WebSocket = require('ws');
const fs = require('fs');
const app = express();
app.use(cors());
app.use(express.json());

const BASE_URL = 'https://api.kiwoom.com';
const cfg = JSON.parse(fs.readFileSync('/Users/hong/proxy/config.json','utf8'));

let kiwoomWs = null;
let currentToken = null;
let sseClients = [];
let realtimePrices = {};
let wsGeneration = 0;

// 탭별 종목 코드 저장소
const WATCHLIST_FILE = '/Users/hong/proxy/watchlist.json';
function loadWatchlist() {
  try { return JSON.parse(require('fs').readFileSync(WATCHLIST_FILE, 'utf8')); } catch(e) { return {holding:[],watch1:[],watch2:[],ready:[]}; }
}
function saveWatchlist() {
  require('fs').writeFileSync(WATCHLIST_FILE, JSON.stringify(watchlistGroups, null, 2));
}
let watchlistGroups = loadWatchlist();

const TELEGRAM_BOT_TOKEN = '8386593712:AAHl8vvQ8mikCsisiMK2BHZORfA9kV1Q7Lw';
const TELEGRAM_CHAT_ID = '5013728591';
const ALERTS_FILE = '/Users/hong/proxy/alerts.json';
function loadAlerts() {
  try { return JSON.parse(require('fs').readFileSync(ALERTS_FILE, 'utf8')); } catch(e) { return {}; }
}
function saveAlerts() {
  require('fs').writeFileSync(ALERTS_FILE, JSON.stringify(alerts, null, 2));
}
let alerts = loadAlerts();
let alertCooldown = {}; // { "stockCode_stop" | "stockCode_target": timestamp }

async function sendTelegram(message) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text: message
    });
    console.log('📨 텔레그램 발송:', message);
  } catch(e) {
    console.log('❌ 텔레그램 발송 실패:', e.message);
  }
}

function checkAlerts(code, price) {
  console.log(`🔍 체크: code=${code} price=${price} alertKeys=${JSON.stringify(Object.keys(alerts))}`);
  const alert = alerts[code];
  if (!alert) return;
  const now = Date.now();
  const cooldownMs = 60 * 60 * 1000;

  if (alert.stopLoss && price <= alert.stopLoss) {
    const key = `${code}_stop`;
    if (!alertCooldown[key] || now - alertCooldown[key] >= cooldownMs) {
      alertCooldown[key] = now;
      sendTelegram(`🚨 [${alert.name}] 손절가 이탈! 현재가 ${price.toLocaleString()}원 (손절가 ${alert.stopLoss.toLocaleString()}원)`);
    }
  }

  if (alert.target && price >= alert.target) {
    const key = `${code}_target`;
    if (!alertCooldown[key] || now - alertCooldown[key] >= cooldownMs) {
      alertCooldown[key] = now;
      console.log(`🎯 목표가 조건 충족! ${alert.name} price=${price} target=${alert.target}`);
      sendTelegram(`🎯 [${alert.name}] 목표가 도달! 현재가 ${price.toLocaleString()}원 (목표가 ${alert.target.toLocaleString()}원)`);
    }
  }
}

function getAllWatchCodes() {
  const all = new Set([
    ...watchlistGroups.holding,
    ...watchlistGroups.watch1,
    ...watchlistGroups.watch2,
    ...watchlistGroups.ready
  ]);
  return [...all].filter(Boolean);
}

function subscribeAllCodes() {
  if (!kiwoomWs || kiwoomWs.readyState !== 1) return;
  const codes = getAllWatchCodes();
  if (codes.length === 0) return;
  kiwoomWs.send(JSON.stringify({
    trnm: 'REG',
    grp_no: '2',
    refresh: '1',
    data: [{ item: codes, type: codes.map(() => '0B') }]
  }));
  console.log(`📋 REG codes: ${JSON.stringify(codes)}`);
  console.log(`📡 전체 구독 갱신 (${codes.length}종목): ${codes.join(', ')}`);
}

function broadcastSSE(data) {
  const msg = `data: ${JSON.stringify(data)}\n\n`;
  sseClients = sseClients.filter(r => !r.writableEnded);
  sseClients.forEach(r => r.write(msg));
}

async function getToken() {
  const res = await axios.post(`${BASE_URL}/oauth2/token`, {
    grant_type: 'client_credentials',
    appkey: cfg.appkey,
    secretkey: cfg.secretkey
  }, { headers: { 'Content-Type': 'application/json;charset=UTF-8' } });
  return res.data.token;
}

function connectKiwoomWS(token) {
  const myGen = ++wsGeneration;
  if (kiwoomWs) { try { kiwoomWs.terminate(); } catch(e) {} }
  currentToken = token;
  kiwoomWs = new WebSocket('wss://api.kiwoom.com:10000/api/dostk/websocket');

  kiwoomWs.on('open', () => {
    console.log('✅ 키움 WS 연결');
    kiwoomWs.send(JSON.stringify({ trnm: 'LOGIN', token: token }));
  });

  kiwoomWs.on('message', (data) => {
    try {
      const raw = data.toString();
      console.log('📥 RAW:', raw.slice(0, 300));
      const msg = JSON.parse(raw);

      if (msg.trnm === 'PING') {
        kiwoomWs.send(JSON.stringify({ trnm: 'PING' }));
        return;
      }

      if (msg.trnm === 'LOGIN' && msg.return_code === 0) {
        console.log('✅ 로그인 성공');
        kiwoomWs.send(JSON.stringify({
          trnm: 'REG',
          grp_no: '1',
          refresh: '1',
          data: [{ item: ['0001', '1001'], type: ['0U', '0U'] }]
        }));
        console.log('📡 KOSPI/KOSDAQ 실시간 등록 요청');
        setTimeout(() => subscribeAllCodes(), 500);
      }

      if (msg.trnm === 'REG') {
        console.log('📡 REG 응답:', JSON.stringify(msg));
        return;
      }

      if (msg.trnm === 'REAL') {
        const items = Array.isArray(msg.data) ? msg.data : [msg.data];
        items.forEach(d => {
          if (!d) return;
          const type = d.type;
          const item = d.item;
          const values = d.values || {};
          const rawPrice = values['10'] || values['현재가'] || '0';
          const price = Math.abs(parseFloat(rawPrice));
          const rawChange = values['12'] || values['등락률'] || '0';
          const change = parseFloat(rawChange);
          if (!item || !price) {
            console.log(`📥 REAL(${type}) 원본:`, JSON.stringify(d).slice(0, 200));
            return;
          }
          if (type === '0U') {
            realtimePrices[item] = { price, change };
            broadcastSSE({ type: 'index', code: item, price, change });
            console.log(`📊 ${item === '0001' ? 'KOSPI' : 'KOSDAQ'}: ${price} (${change > 0 ? '+' : ''}${change}%)`);
          }
          if (type === '0B') {
            realtimePrices[item] = { price, change };
            broadcastSSE({ type: 'price', item, price, change });
            console.log(`📈 ${item}: ${price}원 (${change > 0 ? '+' : ''}${change}%)`);
            checkAlerts(item, price);
          }
        });
        return;
      }

      if (msg.trnm === 'SYSTEM') {
        console.log('⚠️ SYSTEM:', JSON.stringify(msg));
        return;
      }

      console.log('📥 기타 메시지:', JSON.stringify(msg).slice(0, 200));

    } catch(e) {
      console.log('❌ 메시지 파싱 오류:', e.message, '원본:', data.toString().slice(0, 100));
    }
  });

  kiwoomWs.on('error', (err) => console.log('❌ WS 오류:', err.message));
  kiwoomWs.on('close', () => {
    if (myGen !== wsGeneration) return; // 갱신으로 인한 종료 → 재연결 생략
    console.log('🔌 WS 종료, 10초 후 재연결...');
    setTimeout(() => { if (myGen === wsGeneration) connectKiwoomWS(currentToken); }, 10000);
  });
}

async function renewToken() {
  console.log('🔄 토큰 자동 갱신 시작...');
  try {
    const token = await getToken();
    console.log('✅ 토큰 갱신 성공, WebSocket 재연결');
    connectKiwoomWS(token);
  } catch(e) {
    console.log('❌ 토큰 갱신 실패:', e.message, '→ 1시간 후 재시도');
    setTimeout(renewToken, 60 * 60 * 1000);
  }
}

app.get('/sse', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  sseClients.push(res);
  res.write(`data: ${JSON.stringify({ type: 'init', prices: realtimePrices })}\n\n`);
  const heartbeat = setInterval(() => res.write(': ping\n\n'), 30000);
  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients = sseClients.filter(c => c !== res);
  });
  if (currentToken && (!kiwoomWs || kiwoomWs.readyState !== WebSocket.OPEN)) {
    console.log('🔄 SSE 접속 감지 → WebSocket 재연결');
    connectKiwoomWS(currentToken);
  }
});

app.post('/ws/start', (req, res) => {
  const token = req.body.token || currentToken;
  if (!token) return res.status(400).json({ error: 'token 필요' });
  connectKiwoomWS(token);
  res.json({ status: 'ok' });
});

app.post('/ws/register', (req, res) => {
  const { items, type } = req.body;
  if (!kiwoomWs || kiwoomWs.readyState !== 1) {
    return res.status(400).json({ error: 'WebSocket 연결 안됨' });
  }
  kiwoomWs.send(JSON.stringify({
    trnm: 'REG',
    grp_no: '2',
    refresh: '0',
    data: [{ item: items, type: items.map(() => type || '0B') }]
  }));
  res.json({ status: 'ok', registered: items });
});

app.post('/subscribe', (req, res) => {
  const { codes, group } = req.body;
  if (!Array.isArray(codes) || codes.length === 0) {
    return res.status(400).json({ error: 'codes 배열 필요' });
  }
  if (!kiwoomWs || kiwoomWs.readyState !== 1) {
    return res.status(503).json({ error: 'WebSocket 연결 안됨' });
  }
  const groupKey = group || 'holding';
  if (Object.prototype.hasOwnProperty.call(watchlistGroups, groupKey)) {
    watchlistGroups[groupKey] = codes;
    saveWatchlist();
  }
  subscribeAllCodes();
  const all = getAllWatchCodes();
  console.log(`📡 구독 갱신 [${groupKey}: ${codes.length}종목] → 전체 ${all.length}종목`);
  res.json({ status: 'ok', subscribed: all });
});

// 여러 탭 종목을 한번에 업데이트
app.post('/watchlist', (req, res) => {
  const { groups } = req.body; // { holding: [...], watch1: [...], watch2: [...], ready: [...] }
  if (!groups || typeof groups !== 'object') {
    return res.status(400).json({ error: 'groups 객체 필요' });
  }
  ['holding', 'watch1', 'watch2', 'ready'].forEach(key => {
    if (Array.isArray(groups[key])) watchlistGroups[key] = groups[key];
      saveWatchlist();
  });
  if (kiwoomWs && kiwoomWs.readyState === 1) {
    subscribeAllCodes();
  }
  const all = getAllWatchCodes();
  console.log(`📋 watchlist 업데이트 → 전체 ${all.length}종목 (holding:${watchlistGroups.holding.length} watch1:${watchlistGroups.watch1.length} watch2:${watchlistGroups.watch2.length} ready:${watchlistGroups.ready.length})`);
  res.json({ status: 'ok', total: all.length, groups: watchlistGroups });
});

app.get('/watchlist', (req, res) => {
  res.json({ groups: watchlistGroups, all: getAllWatchCodes() });
});

app.get('/prices', (req, res) => {
  res.json(realtimePrices);
});

app.post('/oauth2/token', async (req, res) => {
  try {
    const r = await axios.post(`${BASE_URL}/oauth2/token`, {
      grant_type: 'client_credentials',
      appkey: req.body.appkey,
      secretkey: req.body.secretkey
    }, { headers: { 'Content-Type': 'application/json;charset=UTF-8', 'ngrok-skip-browser-warning': '1' } });
    res.json(r.data);
  } catch(e) { res.status(e.response?.status||500).json({ error: e.message }); }
});

app.post('/api/dostk/:path', async (req, res) => {
  try {
    const r = await axios.post(`${BASE_URL}/api/dostk/${req.params.path}`, req.body, {
      headers: {
        'Content-Type': 'application/json;charset=UTF-8',
        'ngrok-skip-browser-warning': '1',
        'authorization': currentToken ? `Bearer ${currentToken}` : (req.headers['authorization'] || ''),
        'api-id': req.headers['api-id']
      }
    });
    res.json(r.data);
  } catch(e) { res.status(e.response?.status||500).json({ error: e.message }); }
});

app.post('/api/alerts', (req, res) => {
  const { code, name, stopLoss, target } = req.body;
  if (!code || !name) return res.status(400).json({ error: 'code, name 필수' });
  alerts[code] = { name, stopLoss: stopLoss || null, target: target || null };
  delete alertCooldown[`${code}_stop`];
  delete alertCooldown[`${code}_target`];
  saveAlerts();
  console.log(`🔔 알림 등록: ${name}(${code}) 손절가=${stopLoss} 목표가=${target}`);
  res.json({ status: 'ok', alert: alerts[code] });
});

app.delete('/api/alerts/:code', (req, res) => {
  const { code } = req.params;
  delete alerts[code];
  delete alertCooldown[`${code}_stop`];
  delete alertCooldown[`${code}_target`];
  saveAlerts();
  res.json({ status: 'ok' });
});

app.get('/api/alerts', (req, res) => { res.json(alerts); });

app.get('/ping', (req, res) => { res.json({ status: 'ok' }); });

// ===== 시총순위 =====
const MARKET_CAP_FILE = '/Users/hong/proxy/marketcap_snapshot.json';
let marketCapCache = { kospi: [], kosdaq: [], lastUpdated: null };
let marketCapSnapshot = (() => {
  try { return JSON.parse(fs.readFileSync(MARKET_CAP_FILE, 'utf8')); }
  catch(e) { return { date: null, kospi: [], kosdaq: [] }; }
})();
const _mcAlertsToday = { kospi: new Set(), kosdaq: new Set() };

function saveMarketCapSnapshot(data) {
  fs.writeFileSync(MARKET_CAP_FILE, JSON.stringify(data, null, 2));
}

// 네이버 파이낸스로 시총 순위 조회 (키움 REST API에 시총순위 전용 API 없음)
const iconv = require('iconv-lite');

async function fetchMarketCapList(sosok, limit) {
  // sosok: '0' = 코스피, '1' = 코스닥
  const pages = Math.ceil(limit / 50);
  const all = [];

  for (let page = 1; page <= pages && all.length < limit; page++) {
    try {
      const r = await axios.get('https://finance.naver.com/sise/sise_market_sum.naver', {
        params: { sosok, page },
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
          'Accept-Language': 'ko-KR,ko;q=0.9',
          'Referer': 'https://finance.naver.com/sise/sise_market_sum.naver'
        },
        responseType: 'arraybuffer',
        timeout: 10000
      });

      const html = iconv.decode(Buffer.from(r.data), 'EUC-KR');
      const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/g;
      let m;
      while ((m = rowRe.exec(html)) !== null) {
        const row = m[1];
        const codeM = row.match(/code=(\d{6})/);
        const nameM = row.match(/class="tltle"[^>]*>\s*([^<]+)\s*</);
        if (!codeM || !nameM) continue;

        // 숫자 td 값 추출 (콤마 제거)
        const tds = [...row.matchAll(/class="[^"]*"[^>]*>\s*([\d,+-]+)\s*<\/td>/g)]
          .map(t => t[1].replace(/,/g, '').replace(/^\+/, ''));
        if (tds.length < 4) continue;

        const price   = parseInt(tds[1]) || 0;
        const prevChg = parseInt(tds[2]) || 0;     // 전일비(원)
        const cap     = parseInt(tds[3]) || 0;     // 시가총액(억원)
        const prevClose = price - prevChg;
        const rate    = prevClose > 0
          ? ((prevChg / prevClose) * 100).toFixed(2)
          : '0.00';

        all.push({
          stk_cd:   codeM[1],
          stk_nm:   nameM[1].trim(),
          cur_prc:  String(price),
          flu_rt:   rate,
          mrkt_cap: String(cap),
          rank:     parseInt(tds[0]) || all.length + 1
        });
      }
    } catch(e) {
      console.log(`❌ 네이버 시총순위 조회(sosok=${sosok} page=${page}) 실패:`, e.message);
    }
    if (page < pages) await new Promise(res => setTimeout(res, 300));
  }

  return all.slice(0, limit);
}

async function refreshMarketCap() {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [kospiList, kosdaqList] = await Promise.all([
      fetchMarketCapList('0', 100),
      fetchMarketCapList('1', 20)
    ]);
    if (!kospiList.length && !kosdaqList.length) return;

    // 날짜 바뀌면 일중 중복알림 set 초기화
    if (marketCapSnapshot.date && marketCapSnapshot.date !== today) {
      _mcAlertsToday.kospi.clear();
      _mcAlertsToday.kosdaq.clear();
    }

    const prevKospiSet  = new Set((marketCapSnapshot.kospi  || []).map(s => s.stk_cd).filter(Boolean));
    const prevKosdaqSet = new Set((marketCapSnapshot.kosdaq || []).map(s => s.stk_cd).filter(Boolean));

    // 신규 진입 감지 (전일 스냅샷 있을 때만 알림)
    if (marketCapSnapshot.kospi.length > 0) {
      const newKospi = kospiList.filter(s => s.stk_cd && !prevKospiSet.has(s.stk_cd) && !_mcAlertsToday.kospi.has(s.stk_cd));
      if (newKospi.length > 0) {
        newKospi.forEach(s => _mcAlertsToday.kospi.add(s.stk_cd));
        const names = newKospi.map(s => `${s.stk_nm}(${s.stk_cd})`).join(', ');
        await sendTelegram(`📈 [코스피 시총 TOP100 신규진입]\n${names}`);
      }
    }
    if (marketCapSnapshot.kosdaq.length > 0) {
      const newKosdaq = kosdaqList.filter(s => s.stk_cd && !prevKosdaqSet.has(s.stk_cd) && !_mcAlertsToday.kosdaq.has(s.stk_cd));
      if (newKosdaq.length > 0) {
        newKosdaq.forEach(s => _mcAlertsToday.kosdaq.add(s.stk_cd));
        const names = newKosdaq.map(s => `${s.stk_nm}(${s.stk_cd})`).join(', ');
        await sendTelegram(`📈 [코스닥 시총 TOP20 신규진입]\n${names}`);
      }
    }

    marketCapCache = {
      kospi:  kospiList.map(s => ({ ...s, isNew: !prevKospiSet.has(s.stk_cd) })),
      kosdaq: kosdaqList.map(s => ({ ...s, isNew: !prevKosdaqSet.has(s.stk_cd) })),
      lastUpdated: new Date().toISOString(),
      snapshotDate: marketCapSnapshot.date
    };
    console.log(`📊 시총순위 갱신: 코스피 ${kospiList.length}종목, 코스닥 ${kosdaqList.length}종목`);
  } catch(e) {
    console.log('❌ refreshMarketCap 실패:', e.message);
  }
}

// 장마감(15:35~15:45) 시 전일 스냅샷 저장
function _mcSaveSnapshotIfClose() {
  const h = new Date().getHours(), m = new Date().getMinutes();
  if (h === 15 && m >= 35 && m <= 45 && marketCapCache.kospi.length > 0) {
    const today = new Date().toISOString().split('T')[0];
    if (!marketCapSnapshot.date || marketCapSnapshot.date !== today) {
      marketCapSnapshot = {
        date: today,
        kospi:  marketCapCache.kospi.map(({ isNew, ...s }) => s),
        kosdaq: marketCapCache.kosdaq.map(({ isNew, ...s }) => s)
      };
      saveMarketCapSnapshot(marketCapSnapshot);
      console.log(`📸 시총순위 전일 스냅샷 저장 (${today})`);
    }
  }
}

app.get('/api/market-cap', async (req, res) => {
  const force = req.query.force === 'true';
  const age = marketCapCache.lastUpdated
    ? Date.now() - new Date(marketCapCache.lastUpdated).getTime() : Infinity;
  if (!force && age < 5 * 60 * 1000 && marketCapCache.kospi.length > 0) {
    return res.json(marketCapCache);
  }
  await refreshMarketCap();
  res.json(marketCapCache);
});

// 장중(9:05~15:35) 5분마다 자동 갱신
setInterval(() => {
  const h = new Date().getHours(), m = new Date().getMinutes();
  const isMarket = (h === 9 && m >= 5) || (h > 9 && h < 15) || (h === 15 && m <= 35);
  if (isMarket) refreshMarketCap();
  _mcSaveSnapshotIfClose();
}, 5 * 60 * 1000);

app.listen(3000, async () => {
  console.log('✅ 프록시 서버 실행중 : 포트 3000');
  try {
    const token = await getToken();
    console.log('✅ 토큰 발급 성공');
    connectKiwoomWS(token);
    // 23시간마다 토큰 자동 갱신 + WebSocket 재연결
    setInterval(renewToken, 23 * 60 * 60 * 1000);
    console.log('⏰ 토큰 자동 갱신 예약 (23시간 간격)');
  } catch(e) {
    console.log('❌ 토큰 발급 실패:', e.message);
  }
});
