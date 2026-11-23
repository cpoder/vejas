// Admission mock for twilio_sms_send.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9205);
let count = 0;

const POST_BODY = {
  "sid": "SM12345678901234567890123456789012",
  "date_created": "Sat, 23 Nov 2026 12:00:00 +0000",
  "date_updated": "Sat, 23 Nov 2026 12:00:00 +0000",
  "date_sent": "Sat, 23 Nov 2026 12:00:01 +0000",
  "account_sid": "ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
  "to": "+33612345678",
  "from": "+15551234567",
  "messaging_service_sid": null,
  "body": "Your message",
  "status": "sent",
  "num_segments": "1",
  "num_media": "0",
  "direction": "outbound-api",
  "api_version": "2010-04-01",
  "price": "-0.00750",
  "price_unit": "USD",
  "error_code": null,
  "error_message": null,
  "uri": "/2010-04-01/Accounts/ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/Messages/SM12345678901234567890123456789012.json"
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
    
    // Twilio Messages API
    if (req.method === 'POST' && req.url.includes('/Messages.json')) {
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('Basic ')) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          code: 20003,
          message: "Authenticate",
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
