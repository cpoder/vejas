// Admission mock for mongodb_query.
// Note: This is a stated exception (ADR-0017) - MongoDB cannot be meaningfully mocked
// Real testing requires a MongoDB instance
// This mock validates the connector manifest structure only

import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9207);
let count = 0;

createServer((req, res) => {
  if (req.url === '/__count') { 
    res.writeHead(200); 
    res.end(String(count)); 
    return; 
  }
  
  count++;
  res.writeHead(200, {'content-type': 'text/plain'});
  res.end('STATED EXCEPTION: MongoDB requires real database for testing\nSee ADR-0017 for exec-rpc connectors');
}).listen(port, '127.0.0.1', () => console.log('READY', port));
