import http from 'node:http';
import WebSocket from 'ws';

const PORT = process.env.PORT || 3000;

function fetchDerivData({ appId, token, symbol = 'R_100', count = 60, granularity = 1800 }) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(appId)}`);

    const cleanup = () => {
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        ws.close();
      }
    };

    const sendAndWait = (payload, expectedType) =>
      new Promise((res, rej) => {
        const onMessage = (raw) => {
          const msg = JSON.parse(raw.toString());
          if (msg.error) {
            ws.off('message', onMessage);
            rej(new Error(msg.error.message || 'Deriv API error'));
            return;
          }
          if (msg.msg_type === expectedType) {
            ws.off('message', onMessage);
            res(msg);
          }
        };
        ws.on('message', onMessage);
        ws.send(JSON.stringify(payload));
      });

    ws.once('error', (err) => {
      cleanup();
      reject(err);
    });

    ws.once('open', async () => {
      try {
        const authRes = await sendAndWait({ authorize: token }, 'authorize');
        const candlesRes = await sendAndWait(
          {
            ticks_history: symbol,
            adjust_start_time: 1,
            count,
            end: 'latest',
            style: 'candles',
            granularity
          },
          'candles'
        );
        cleanup();
        resolve({
          balance: authRes.authorize.balance,
          currency: authRes.authorize.currency,
          symbol,
          candles: candlesRes.candles || []
        });
      } catch (err) {
        cleanup();
        reject(err);
      }
    });
  });
}

http
  .createServer(async (req, res) => {
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Use POST' }));
      return;
    }

    let body = '';
    for await (const chunk of req) body += chunk;

    try {
      const parsed = JSON.parse(body || '{}');
      const data = await fetchDerivData(parsed);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(data));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
  })
  .listen(PORT, () => console.log(`Bridge listening on ${PORT}`));
