// Admission mock for jira_create_issue.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9212);
let count = 0;

const POST_BODY = {
  "id": "10042",
  "key": "PROJ-123",
  "self": "https://your-domain.atlassian.net/rest/api/3/issue/10042",
  "fields": {
    "summary": "Bug report",
    "description": {
      "type": "doc",
      "version": 1,
      "content": [{"type": "paragraph", "content": [{"type": "text", "text": "Details"}]}]
    },
    "issuetype": {"id": "10001", "name": "Bug", "subtask": false},
    "project": {"id": "10000", "key": "PROJ", "name": "Project Name"},
    "priority": {"id": "2", "name": "High"},
    "status": {"id": "1", "name": "To Do"},
    "created": "2026-11-23T12:00:00.000+0000",
    "updated": "2026-11-23T12:00:00.000+0000",
    "reporter": {
      "accountId": "5b10a2844c20165700ede21g",
      "displayName": "Test User",
      "emailAddress": "test@example.com"
    }
  }
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
    
    // Jira Issues API
    if (req.method === 'POST' && req.url.includes('/issue')) {
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('Basic ')) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          errorMessages: ["You do not have the permission to see the specified issue."],
          errors: {}
        }));
        return;
      }
      
      res.writeHead(201, {'content-type': 'application/json'});
      res.end(JSON.stringify(POST_BODY));
    } else {
      res.writeHead(405, {'content-type': 'application/json'});
      res.end(JSON.stringify({errorMessages: ['Method not allowed']}));
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));