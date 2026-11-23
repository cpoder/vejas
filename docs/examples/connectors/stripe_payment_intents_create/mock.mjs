// Admission mock for stripe_payment_intents_create.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9201);
let count = 0;

const POST_BODY = {
  "id": "pi_3QABCDabcdef123456",
  "object": "payment_intent",
  "amount": 5000,
  "amount_capturable": 0,
  "amount_received": 0,
  "currency": "eur",
  "customer": "cus_QABCDabcdef123",
  "description": "Test payment from Vejas",
  "status": "requires_payment_method",
  "created": 1700000000,
  "livemode": false,
  "client_secret": "pi_3QABCDabcdef123456_secret_abcdefghijklmnop"
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
    
    // Stripe API expects POST with form-encoded data
    if (req.method === 'POST' && req.url === '/v1/payment_intents') {
      // Check Authorization header
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('Bearer ')) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          error: {
            type: 'invalid_request_error',
            message: 'You did not provide an API key.'
          }
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
