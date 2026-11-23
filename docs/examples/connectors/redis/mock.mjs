#!/usr/bin/env node
// Mock Redis exec-rpc wrapper for admission testing
// Simulates vejas-redis binary that accepts JSON requests on stdin

import readline from 'readline';

const store = new Map();
store.set('user:123', 'John Doe');
store.set('counter', '42');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

rl.on('line', (line) => {
  try {
    const request = JSON.parse(line);
    
    if (request.op === 'get') {
      // GET key
      const value = store.get(request.key);
      const response = {
        ok: true,
        value: value || null,
        type: 'string'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'set') {
      // SET key value [EX seconds]
      store.set(request.key, request.value);
      const response = {
        ok: true,
        result: 'OK'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'del') {
      // DEL key
      const deleted = store.delete(request.key);
      const response = {
        ok: true,
        deleted: deleted ? 1 : 0
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'exists') {
      // EXISTS key
      const exists = store.has(request.key);
      const response = {
        ok: true,
        exists: exists ? 1 : 0
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'hgetall') {
      // HGETALL key
      const response = {
        ok: true,
        fields: {
          name: 'John Doe',
          email: 'john@example.com',
          age: '32'
        }
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'hset') {
      // HSET key field value
      const response = {
        ok: true,
        result: 1
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'lpush' || request.op === 'rpush') {
      // LPUSH/RPUSH key value
      const response = {
        ok: true,
        length: 1
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'lrange') {
      // LRANGE key start stop
      const response = {
        ok: true,
        values: ['item1', 'item2', 'item3']
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'sadd') {
      // SADD key member
      const response = {
        ok: true,
        added: 1
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'smembers') {
      // SMEMBERS key
      const response = {
        ok: true,
        members: ['member1', 'member2']
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'publish') {
      // PUBLISH channel message
      const response = {
        ok: true,
        subscribers: 0
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'eval') {
      // EVAL script numkeys key [key ...] arg [arg ...]
      const response = {
        ok: true,
        result: 'John Doe'
      };
      console.log(JSON.stringify(response));
    } else {
      console.log(JSON.stringify({
        ok: false,
        error: `Unknown operation: ${request.op}`
      }));
    }
  } catch (err) {
    console.log(JSON.stringify({
      ok: false,
      error: err.message
    }));
  }
});

console.error('Mock Redis connector started');
