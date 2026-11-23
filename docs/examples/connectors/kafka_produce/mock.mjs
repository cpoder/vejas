// Admission mock for kafka_produce.
// Note: This is a stated exception (ADR-0017, ADR-0022) - Kafka cannot be meaningfully mocked
// Real testing requires a Kafka broker
// This mock validates the connector manifest structure only

import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9210);
let count = 0;

createServer((req, res) => {
  if (req.url === '/__count') { 
    res.writeHead(200); 
    res.end(String(count)); 
    return; 
  }
  
  count++;
  res.writeHead(200, {'content-type': 'text/plain'});
  res.end('STATED EXCEPTION: Kafka requires real broker for testing\nSee ADR-0022 for Kafka connector and ADR-0017 for exec-sink connectors');
}).listen(port, '127.0.0.1', () => console.log('READY', port));
