// Admission mock for azure_blob_storage_upload.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9204);
let count = 0;

createServer((req, res) => {
  if (req.url === '/__count') { 
    res.writeHead(200); 
    res.end(String(count)); 
    return; 
  }
  
  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', () => {
    count++;
    
    // Azure Blob PUT
    if (req.method === 'PUT') {
      const auth = req.headers['authorization'];
      const version = req.headers['x-ms-version'];
      
      if (!auth || !auth.startsWith('SharedKey ')) {
        res.writeHead(403, {'content-type': 'application/xml'});
        res.end('<?xml version="1.0"?><Error><Code>AuthenticationFailed</Code></Error>');
        return;
      }
      
      if (!version) {
        res.writeHead(400, {'content-type': 'application/xml'});
        res.end('<?xml version="1.0"?><Error><Code>MissingRequiredHeader</Code></Error>');
        return;
      }
      
      res.writeHead(201, {
        'content-length': '0',
        'last-modified': 'Sat, 23 Nov 2026 12:00:00 GMT',
        'etag': '"0x8DCABCDEF123456"',
        'content-md5': 'rL0Y20zC+Fzt72VPzMSk2A==',
        'x-ms-request-id': '12345678-1234-1234-1234-123456789012',
        'x-ms-version': '2021-08-06',
        'x-ms-request-server-encrypted': 'true',
        'date': 'Sat, 23 Nov 2026 12:00:00 GMT'
      });
      res.end('');
    } else {
      res.writeHead(405, {'content-type': 'application/xml'});
      res.end('<?xml version="1.0"?><Error><Code>MethodNotAllowed</Code></Error>');
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
