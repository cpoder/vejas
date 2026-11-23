// Admission mock for aws_s3_upload.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9202);
let count = 0;

const PUT_BODY = {
  "ETag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "VersionId": "null",
  "ServerSideEncryption": "AES256",
  "Location": "https://my-bucket.s3.amazonaws.com/path/file.txt"
};

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
    
    // S3 PUT object
    if (req.method === 'PUT') {
      // Check Authorization header (simplified)
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('AWS4-HMAC-SHA256')) {
        res.writeHead(403, {'content-type': 'application/xml'});
        res.end('<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>');
        return;
      }
      
      res.writeHead(200, {
        'content-type': 'application/xml',
        'etag': '"d41d8cd98f00b204e9800998ecf8427e"',
        'x-amz-server-side-encryption': 'AES256'
      });
      res.end('');
    } else {
      res.writeHead(405, {'content-type': 'application/xml'});
      res.end('<?xml version="1.0"?><Error><Code>MethodNotAllowed</Code></Error>');
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
