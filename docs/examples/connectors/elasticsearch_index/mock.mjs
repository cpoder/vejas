// Admission mock for elasticsearch_index.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9209);
let count = 0;

const POST_BODY = {
  "_index": "logs-2026.11",
  "_id": "doc123",
  "_version": 1,
  "result": "created",
  "_shards": {
    "total": 2,
    "successful": 1,
    "failed": 0
  },
  "_seq_no": 0,
  "_primary_term": 1
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
    
    // Elasticsearch index API
    if (req.method === 'POST' || req.method === 'PUT') {
      const auth = req.headers['authorization'];
      if (!auth || (!auth.startsWith('ApiKey ') && !auth.startsWith('Basic '))) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          error: {
            type: "security_exception",
            reason: "missing authentication credentials"
          },
          status: 401
        }));
        return;
      }
      
      res.writeHead(201, {'content-type': 'application/json'});
      res.end(JSON.stringify(POST_BODY));
    } else {
      res.writeHead(405, {'content-type': 'application/json'});
      res.end(JSON.stringify({error: 'Method not allowed'}));
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
