#!/usr/bin/env node
// Mock Twilio API server for admission testing
// Simulates /2010-04-01/Accounts/{AccountSid}/Messages.json endpoint

import http from 'http';

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  // POST /2010-04-01/Accounts/{AccountSid}/Messages.json - send SMS
  if (req.method === 'POST' && url.pathname.match(/^\/2010-04-01\/Accounts\/[^\/]+\/Messages\.json$/)) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      // Parse URL-encoded form data
      const params = new URLSearchParams(body);
      const to = params.get('To');
      const from = params.get('From');
      const messageBody = params.get('Body');
      
      if (!to || !from || !messageBody) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          code: 21602,
          message: 'Message body is required',
          status: 400
        }));
        return;
      }
      
      const now = new Date().toUTCString();
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        account_sid: 'ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        api_version: '2010-04-01',
        body: messageBody,
        date_created: now,
        date_sent: now,
        date_updated: now,
        direction: 'outbound-api',
        error_code: null,
        error_message: null,
        from: from,
        messaging_service_sid: null,
        num_media: '0',
        num_segments: '1',
        price: '-0.00750',
        price_unit: 'USD',
        sid: 'SM' + Date.now().toString(36).toUpperCase(),
        status: 'sent',
        subresource_uris: {
          media: `/2010-04-01/Accounts/ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/Messages/SM${Date.now()}/Media.json`
        },
        to: to,
        uri: `/2010-04-01/Accounts/ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/Messages/SM${Date.now()}.json`
      }));
    });
    return;
  }

  // GET /2010-04-01/Accounts/{AccountSid}/Messages.json - list messages
  if (req.method === 'GET' && url.pathname.match(/^\/2010-04-01\/Accounts\/[^\/]+\/Messages\.json$/)) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      messages: [
        {
          account_sid: 'ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
          sid: 'SMXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
          body: 'Test message',
          from: '+15551234567',
          to: '+15559876543',
          status: 'delivered',
          date_created: new Date().toUTCString()
        }
      ],
      page: 0,
      page_size: 50,
      uri: '/2010-04-01/Accounts/ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/Messages.json'
    }));
    return;
  }

  // POST /2010-04-01/Accounts/{AccountSid}/Calls.json - make call
  if (req.method === 'POST' && url.pathname.match(/^\/2010-04-01\/Accounts\/[^\/]+\/Calls\.json$/)) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      const params = new URLSearchParams(body);
      const to = params.get('To');
      const from = params.get('From');
      const url = params.get('Url');
      
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        account_sid: 'ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        sid: 'CA' + Date.now().toString(36).toUpperCase(),
        from: from,
        to: to,
        status: 'queued',
        direction: 'outbound-api',
        date_created: new Date().toUTCString()
      }));
    });
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    code: 20404,
    message: 'The requested resource was not found',
    status: 404
  }));
});

const PORT = process.env.PORT || 8082;
server.listen(PORT, () => {
  console.log(`Mock Twilio API listening on port ${PORT}`);
});
