// Admission mock for redis_command.
// Note: This is a stated exception (ADR-0017) - Redis cannot be meaningfully mocked
// Real testing requires a Redis instance
// This mock validates the connector manifest structure only

import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9208);
let count = 0;

createServer((req, res) => {
  if (req.url === '/__count') { 
    res.writeHead(200); 
    res.end(String(count)); 
    return; 
  }
  
  count++;
  res.writeHead(200, {'content-type': 'text/plain'});
  res.end('STATED EXCEPTION: Redis requires real instance for testing\nSee ADR-0017 for exec-rpc connectors');
}).listen(port, '127.0.0.1', () => console.log('READY', port));
