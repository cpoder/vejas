#!/usr/bin/env node
// Mock AWS S3 API server for admission testing
// Simulates basic S3 operations (PUT, GET, DELETE objects)

import http from 'http';

const buckets = {
  'my-bucket': {}
};

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, DELETE, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, x-amz-date, x-amz-content-sha256, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathParts = url.pathname.split('/').filter(p => p);
  
  // PUT /{bucket}/{key} - upload object
  if (req.method === 'PUT' && pathParts.length >= 2) {
    const bucket = pathParts[0];
    const key = pathParts.slice(1).join('/');
    
    if (!buckets[bucket]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
  <BucketName>${bucket}</BucketName>
</Error>`);
      return;
    }
    
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      buckets[bucket][key] = body;
      
      const etag = '"' + Buffer.from(body).toString('base64').substring(0, 32) + '"';
      res.writeHead(200, {
        'Content-Type': 'application/xml',
        'ETag': etag,
        'x-amz-request-id': 'EXAMPLE123456789',
        'x-amz-id-2': 'EXAMPLEHOSTID123456789',
        'x-amz-server-side-encryption': 'AES256'
      });
      res.end();
    });
    return;
  }

  // GET /{bucket}/{key} - get object
  if (req.method === 'GET' && pathParts.length >= 2) {
    const bucket = pathParts[0];
    const key = pathParts.slice(1).join('/');
    
    if (!buckets[bucket]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
</Error>`);
      return;
    }
    
    if (!buckets[bucket][key]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist</Message>
  <Key>${key}</Key>
</Error>`);
      return;
    }
    
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'x-amz-request-id': 'EXAMPLE123456789'
    });
    res.end(buckets[bucket][key]);
    return;
  }

  // DELETE /{bucket}/{key} - delete object
  if (req.method === 'DELETE' && pathParts.length >= 2) {
    const bucket = pathParts[0];
    const key = pathParts.slice(1).join('/');
    
    if (buckets[bucket] && buckets[bucket][key]) {
      delete buckets[bucket][key];
    }
    
    res.writeHead(204, {
      'x-amz-request-id': 'EXAMPLE123456789',
      'x-amz-id-2': 'EXAMPLEHOSTID123456789'
    });
    res.end();
    return;
  }

  // GET /{bucket} - list objects
  if (req.method === 'GET' && pathParts.length === 1) {
    const bucket = pathParts[0];
    
    if (!buckets[bucket]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>The specified bucket does not exist</Message>
</Error>`);
      return;
    }
    
    const keys = Object.keys(buckets[bucket]);
    const contents = keys.map(key => `
  <Contents>
    <Key>${key}</Key>
    <LastModified>2026-11-23T12:00:00.000Z</LastModified>
    <ETag>"${Buffer.from(key).toString('base64').substring(0, 32)}"</ETag>
    <Size>${buckets[bucket][key].length}</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>`).join('');
    
    res.writeHead(200, { 'Content-Type': 'application/xml' });
    res.end(`<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>${bucket}</Name>
  <Prefix></Prefix>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>${contents}
</ListBucketResult>`);
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/xml' });
  res.end(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified resource does not exist</Message>
</Error>`);
});

const PORT = process.env.PORT || 8084;
server.listen(PORT, () => {
  console.log(`Mock AWS S3 API listening on port ${PORT}`);
});
