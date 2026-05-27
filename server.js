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
      const msg = JSON.parse(data.toString());

      // PING 응답
      if (msg.trnm === 'PING') {
        kiwoomWs.send(JSON.stringify({ trnm: 'PING' }));
        return;
      }

      // 로그인 성공 → 종목 등록
      // 0U: 업종등락 (KOSPI=0001, KOSDAQ=1001)
      // 0B: 주식체결 (개별종목)
      if (msg.trnm === 'LOGIN' && msg.return_code === 0) {
        console.log('✅ 로그인 성공');
        // 업종지수 실시간 등록
        kiwoomWs.send(JSON.stringify({
          trnm: 'REG',
          grp_no: '1',
          refresh: '1',
          data: [{ item: ['0001', '1001'], type: ['0U', '0U'] }]
        }));
        console.log('📡 KOSPI/KOSDAQ 실시간 등록 요청');
      }

      // REG 응답 확인
      if (msg.trnm === 'REG') {
        console.log('📡 REG 응답:', JSON.stringify(msg));
      }

      // 업종등락 실시간 데이터 (0U)
      if (msg.trnm === '0U') {
        const code = msg.data?.['업종코드'];
        const price = Math.abs(parseFloat(msg.data?.['현재가'] || msg.data?.['--10'] || 0));
        const change = parseFloat(msg.data?.['등락률'] || msg.data?.['--12'] || 0);
        if (code && price) {
          realtimePrices[code] = { price, change };
          broadcastSSE({ type: 'index', code, price, change });
          console.log(`📊 ${code === '0001' ? 'KOSPI' : 'KOSDAQ'}: ${price} (${change}%)`);
        } else {
          // 필드명 디버깅용
          console.log('📊 0U 원본데이터:', JSON.stringify(msg.data).slice(0, 200));
        }
      }

      // 주식체결 실시간 데이터 (0B) - 개별종목
      if (msg.trnm === '0B') {
        const d = msg.data || {};
        const item = d['단축코드'] || d['MKSC_SHRN_ISCD'] || d['stk_cd'];
        const price = Math.abs(parseFloat(d['현재가'] || d['STCK_PRPR'] || d['cur_prc'] || 0));
        const change = parseFloat(d['등락률'] || d['PRDY_CTRT'] || d['flu_rt'] || 0);
        if (item && price) {
          realtimePrices[item] = { price, change };
          broadcastSSE({ type: 'price', item, price, change });
          console.log(`📈 ${item}: ${price}원 (${change > 0 ? '+' : ''}${change}%)`);
        } else {
          console.log('📈 0B 원본:', JSON.stringify(msg).slice(0, 300));
        }
      }

      // SYSTEM 메시지 (오류 등)
      if (msg.trnm === 'SYSTEM') {
        console.log('⚠️ SYSTEM:', JSON.stringify(msg));
      }

    } catch(e) {
      console.log('❌ 메시지 파싱 오류:', e.message);
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

// SSE 엔드포인트
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
  // SSE 접속 시 WebSocket이 끊겨 있으면 자동 재연결
  if (currentToken && (!kiwoomWs || kiwoomWs.readyState !== WebSocket.OPEN)) {
    console.log('🔄 SSE 접속 감지 → WebSocket 재연결');
    connectKiwoomWS(currentToken);
  }
});

// WebSocket 수동 시작
app.post('/ws/start', (req, res) => {
  const token = req.body.token || currentToken;
  if (!token) return res.status(400).json({ error: 'token 필요' });
  connectKiwoomWS(token);
  res.json({ status: 'ok' });
});

// 종목 추가 등록 (웹앱에서 관심종목 실시간 요청)
app.post('/ws/register', (req, res) => {
  const { items, type } = req.body; // items: ['005930','000660'], type: '0B'
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

// 구독 종목 전체 교체 (웹앱 관심종목/ETF 변경 시 자동 호출)
// POST /subscribe { codes: ['005930', '441640', ...] }
app.post('/subscribe', (req, res) => {
  const { codes } = req.body;
  if (!Array.isArray(codes) || codes.length === 0) {
    return res.status(400).json({ error: 'codes 배열 필요' });
  }
  if (!kiwoomWs || kiwoomWs.readyState !== 1) {
    return res.status(503).json({ error: 'WebSocket 연결 안됨' });
  }
  kiwoomWs.send(JSON.stringify({
    trnm: 'REG',
    grp_no: '2',
    refresh: '1',
    data: [{ item: codes, type: codes.map(() => '0B') }]
  }));
  console.log(`📡 구독 갱신 (${codes.length}종목): ${codes.join(', ')}`);
  res.json({ status: 'ok', subscribed: codes });
});

// 현재 실시간 가격 조회
app.get('/prices', (req, res) => {
  res.json(realtimePrices);
});

// 키움 토큰 발급
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

// 키움 REST API 프록시
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

app.get('/ping', (req, res) => { res.json({ status: 'ok' }); });

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
