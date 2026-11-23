#!/usr/bin/env node
// Mock Google Cloud Storage API server for admission testing
// Simulates /storage/v1/b/{bucket}/o endpoint

import http from 'http';

const buckets = {
  'my-bucket': {}
};

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, x-goog-project-id');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  // POST /upload/storage/v1/b/{bucket}/o - upload object
  if (req.method === 'POST' && url.pathname.match(/^\/upload\/storage\/v1\/b\/[^\/]+\/o$/)) {
    const bucket = url.pathname.split('/')[5];
    const name = url.searchParams.get('name');
    
    if (!buckets[bucket]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: {
          code: 404,
          message: 'Not Found'
        }
      }));
      return;
    }
    
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      buckets[bucket][name] = body;
      
      const now = new Date().toISOString();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        kind: 'storage#object',
        id: `${bucket}/${name}/${Date.now()}000000`,
        selfLink: `https://www.googleapis.com/storage/v1/b/${bucket}/o/${encodeURIComponent(name)}`,
        name: name,
        bucket: bucket,
        generation: `${Date.now()}000000`,
        metageneration: '1',
        contentType: 'application/octet-stream',
        storageClass: 'STANDARD',
        size: String(body.length),
        md5Hash: Buffer.from(body).toString('base64').substring(0, 24),
        crc32c: 'AAAAAA==',
        etag: 'CKiG9oDEhvQCEAE=',
        timeCreated: now,
        updated: now,
        timeStorageClassUpdated: now
      }));
    });
    return;
  }

  // GET /storage/v1/b/{bucket}/o - list objects
  if (req.method === 'GET' && url.pathname.match(/^\/storage\/v1\/b\/[^\/]+\/o$/)) {
    const bucket = url.pathname.split('/')[4];
    
    if (!buckets[bucket]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: {
          code: 404,
          message: 'Not Found'
        }
      }));
      return;
    }
    
    const items = Object.keys(buckets[bucket]).map(name => ({
      kind: 'storage#object',
      id: `${bucket}/${name}/${Date.now()}000000`,
      name: name,
      bucket: bucket,
      size: String(buckets[bucket][name].length),
      updated: new Date().toISOString()
    }));
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      kind: 'storage#objects',
      items: items
    }));
    return;
  }

  // GET /storage/v1/b/{bucket}/o/{object} - get object metadata
  if (req.method === 'GET' && url.pathname.match(/^\/storage\/v1\/b\/[^\/]+\/o\/[^\/]+$/)) {
    const parts = url.pathname.split('/');
    const bucket = parts[4];
    const object = decodeURIComponent(parts[6]);
    
    if (!buckets[bucket] || !buckets[bucket][object]) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: {
          code: 404,
          message: 'Not Found'
        }
      }));
      return;
    }
    
    const now = new Date().toISOString();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      kind: 'storage#object',
      id: `${bucket}/${object}/${Date.now()}000000`,
      name: object,
      bucket: bucket,
      size: String(buckets[bucket][object].length),
      updated: now
    }));
    return;
  }

  // DELETE /storage/v1/b/{bucket}/o/{object} - delete object
  if (req.method === 'DELETE' && url.pathname.match(/^\/storage\/v1\/b\/[^\/]+\/o\/[^\/]+$/)) {
    const parts = url.pathname.split('/');
    const bucket = parts[4];
    const object = decodeURIComponent(parts[6]);
    
    if (buckets[bucket] && buckets[bucket][object]) {
      delete buckets[bucket][object];
    }
    
    res.writeHead(204);
    res.end();
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    error: {
      code: 404,
      message: 'Not Found'
    }
  }));
});

const PORT = process.env.PORT || 8085;
server.listen(PORT, () => {
  console.log(`Mock Google Cloud Storage API listening on port ${PORT}`);
});
