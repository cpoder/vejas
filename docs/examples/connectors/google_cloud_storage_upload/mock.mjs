// Admission mock for google_cloud_storage_upload.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9203);
let count = 0;

const POST_BODY = {
  "kind": "storage#object",
  "id": "my-bucket/path/file.txt/1700000000000000",
  "selfLink": "https://www.googleapis.com/storage/v1/b/my-bucket/o/path%2Ffile.txt",
  "name": "path/file.txt",
  "bucket": "my-bucket",
  "generation": "1700000000000000",
  "metageneration": "1",
  "contentType": "text/plain",
  "timeCreated": "2026-11-23T12:00:00.000Z",
  "updated": "2026-11-23T12:00:00.000Z",
  "storageClass": "STANDARD",
  "size": "1024",
  "md5Hash": "rL0Y20zC+Fzt72VPzMSk2A==",
  "mediaLink": "https://storage.googleapis.com/download/storage/v1/b/my-bucket/o/path%2Ffile.txt?generation=1700000000000000&alt=media",
  "crc32c": "AAAAAA=="
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
    
    // GCS upload
    if (req.method === 'POST') {
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('Bearer ')) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          error: {
            code: 401,
            message: "Invalid Credentials"
          }
        }));
        return;
      }
      
      res.writeHead(200, {'content-type': 'application/json'});
      res.end(JSON.stringify(POST_BODY));
    } else {
      res.writeHead(405, {'content-type': 'application/json'});
      res.end(JSON.stringify({error: 'Method not allowed'}));
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
