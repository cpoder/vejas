#!/usr/bin/env node
// Mock Stripe API server for admission testing
// Simulates /v1/events endpoint with cursor pagination

import http from 'http';

const events = [
  {
    id: 'evt_1QRmK2LkdIwHu7ixZvPGmuGU',
    object: 'event',
    api_version: '2023-10-16',
    created: 1732368730,
    data: {
      object: {
        id: 'pi_3QRmK1LkdIwHu7ix0B8tBfzF',
        object: 'payment_intent',
        amount: 2000,
        amount_received: 2000,
        currency: 'usd',
        customer: 'cus_RQvWzKV4cOOYYY',
        description: 'Payment for order #12345',
        status: 'succeeded',
        metadata: {
          order_id: '12345',
          customer_email: 'customer@example.com'
        }
      }
    },
    livemode: false,
    type: 'payment_intent.succeeded'
  },
  {
    id: 'evt_2QRmK3LkdIwHu7ixZvPGmuGV',
    object: 'event',
    api_version: '2023-10-16',
    created: 1732368731,
    data: {
      object: {
        id: 'sub_1QRmK2LkdIwHu7ixYYYYYYYY',
        object: 'subscription',
        customer: 'cus_RQvWzKV4cOOYYY',
        status: 'active',
        current_period_start: 1732368731,
        current_period_end: 1735047131
      }
    },
    livemode: false,
    type: 'customer.subscription.created'
  }
];

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Stripe-Version, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  // GET /v1/events - list events with pagination
  if (req.method === 'GET' && url.pathname === '/v1/events') {
    const limit = parseInt(url.searchParams.get('limit') || '10');
    const startingAfter = url.searchParams.get('starting_after');
    
    let startIndex = 0;
    if (startingAfter) {
      const afterIndex = events.findIndex(e => e.id === startingAfter);
      startIndex = afterIndex >= 0 ? afterIndex + 1 : 0;
    }
    
    const data = events.slice(startIndex, startIndex + limit);
    const hasMore = startIndex + limit < events.length;
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      object: 'list',
      data,
      has_more: hasMore,
      url: '/v1/events'
    }));
    return;
  }

  // GET /v1/events/:id - retrieve single event
  if (req.method === 'GET' && url.pathname.startsWith('/v1/events/')) {
    const eventId = url.pathname.split('/').pop();
    const event = events.find(e => e.id === eventId);
    
    if (event) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(event));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: {
          type: 'invalid_request_error',
          message: 'No such event'
        }
      }));
    }
    return;
  }

  // POST /v1/payment_intents - create payment intent
  if (req.method === 'POST' && url.pathname === '/v1/payment_intents') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        id: 'pi_' + Date.now(),
        object: 'payment_intent',
        amount: 2000,
        currency: 'usd',
        status: 'requires_payment_method',
        created: Math.floor(Date.now() / 1000)
      }));
    });
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    error: {
      type: 'invalid_request_error',
      message: 'Unrecognized request URL'
    }
  }));
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Mock Stripe API listening on port ${PORT}`);
});
