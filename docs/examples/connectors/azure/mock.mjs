#!/usr/bin/env node
// Mock Azure Blob Storage API server for admission testing
// Simulates /{container}/{blob} endpoint

import http from 'http';

const containers = {
  'uploads': {}
};

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, DELETE, HEAD, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, x-ms-date, x-ms-version, x-ms-blob-type, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathParts = url.pathname.split('/').filter(p => p);
  
  // PUT /{container}/{blob} - upload blob
  if (req.method === 'PUT' && pathParts.length >= 2) {
    const container = pathParts[0];
    const blob = pathParts.slice(1).join('/');
    
    if (!containers[container]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ContainerNotFound</Code>
  <Message>The specified container does not exist.</Message>
</Error>`);
      return;
    }
    
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      containers[container][blob] = body;
      
      const now = new Date().toUTCString();
      res.writeHead(201, {
        'Content-Length': '0',
        'Last-Modified': now,
        'ETag': '"0x8DCEB' + Date.now().toString(16).toUpperCase() + '"',
        'x-ms-request-id': '12345678-1234-1234-1234-123456789012',
        'x-ms-version': '2021-08-06',
        'x-ms-blob-type': 'BlockBlob',
        'x-ms-server-encrypted': 'true',
        'Date': now
      });
      res.end();
    });
    return;
  }

  // GET /{container}/{blob} - get blob
  if (req.method === 'GET' && pathParts.length >= 2) {
    const container = pathParts[0];
    const blob = pathParts.slice(1).join('/');
    
    if (!containers[container]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ContainerNotFound</Code>
  <Message>The specified container does not exist.</Message>
</Error>`);
      return;
    }
    
    if (!containers[container][blob]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>BlobNotFound</Code>
  <Message>The specified blob does not exist.</Message>
</Error>`);
      return;
    }
    
    const now = new Date().toUTCString();
    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': String(containers[container][blob].length),
      'Last-Modified': now,
      'ETag': '"0x8DCEB' + Date.now().toString(16).toUpperCase() + '"',
      'x-ms-request-id': '12345678-1234-1234-1234-123456789012',
      'x-ms-version': '2021-08-06',
      'x-ms-blob-type': 'BlockBlob',
      'Date': now
    });
    res.end(containers[container][blob]);
    return;
  }

  // DELETE /{container}/{blob} - delete blob
  if (req.method === 'DELETE' && pathParts.length >= 2) {
    const container = pathParts[0];
    const blob = pathParts.slice(1).join('/');
    
    if (containers[container] && containers[container][blob]) {
      delete containers[container][blob];
    }
    
    const now = new Date().toUTCString();
    res.writeHead(202, {
      'x-ms-request-id': '12345678-1234-1234-1234-123456789012',
      'x-ms-version': '2021-08-06',
      'Date': now
    });
    res.end();
    return;
  }

  // GET /{container}?restype=container&comp=list - list blobs
  if (req.method === 'GET' && pathParts.length === 1 && url.searchParams.get('restype') === 'container' && url.searchParams.get('comp') === 'list') {
    const container = pathParts[0];
    
    if (!containers[container]) {
      res.writeHead(404, { 'Content-Type': 'application/xml' });
      res.end(`<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ContainerNotFound</Code>
  <Message>The specified container does not exist.</Message>
</Error>`);
      return;
    }
    
    const blobs = Object.keys(containers[container]).map(name => `
    <Blob>
      <Name>${name}</Name>
      <Properties>
        <Last-Modified>${new Date().toUTCString()}</Last-Modified>
        <Etag>"0x8DCEB${Date.now().toString(16).toUpperCase()}"</Etag>
        <Content-Length>${containers[container][name].length}</Content-Length>
        <Content-Type>application/octet-stream</Content-Type>
        <BlobType>BlockBlob</BlobType>
      </Properties>
    </Blob>`).join('');
    
    res.writeHead(200, { 'Content-Type': 'application/xml' });
    res.end(`<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs>${blobs}
  </Blobs>
</EnumerationResults>`);
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/xml' });
  res.end(`<?xml version="1.0" encoding="utf-8"?>
<Error>
  <Code>ResourceNotFound</Code>
  <Message>The specified resource does not exist.</Message>
</Error>`);
});

const PORT = process.env.PORT || 8086;
server.listen(PORT, () => {
  console.log(`Mock Azure Blob Storage API listening on port ${PORT}`);
});
