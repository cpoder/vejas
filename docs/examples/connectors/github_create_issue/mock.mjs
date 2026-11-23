// Admission mock for github_create_issue.
import { createServer } from 'node:http';
const port = Number(process.argv[2] || 9211);
let count = 0;

const POST_BODY = {
  "id": 1234567890,
  "node_id": "I_kwDOABCDEF5ABCDEFG",
  "url": "https://api.github.com/repos/myorg/myrepo/issues/42",
  "repository_url": "https://api.github.com/repos/myorg/myrepo",
  "html_url": "https://github.com/myorg/myrepo/issues/42",
  "number": 42,
  "state": "open",
  "title": "Bug report",
  "body": "Description",
  "user": {
    "login": "octocat",
    "id": 1,
    "avatar_url": "https://github.com/images/error/octocat_happy.gif",
    "type": "User"
  },
  "labels": [{"id": 208045946, "name": "bug", "color": "d73a4a"}],
  "assignees": [{"login": "user1", "id": 2}],
  "milestone": null,
  "locked": false,
  "comments": 0,
  "created_at": "2026-11-23T12:00:00Z",
  "updated_at": "2026-11-23T12:00:00Z",
  "closed_at": null,
  "author_association": "OWNER"
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
    
    // GitHub Issues API
    if (req.method === 'POST' && req.url.includes('/issues')) {
      const auth = req.headers['authorization'];
      if (!auth || !auth.startsWith('Bearer ')) {
        res.writeHead(401, {'content-type': 'application/json'});
        res.end(JSON.stringify({
          message: "Requires authentication",
          documentation_url: "https://docs.github.com/rest"
        }));
        return;
      }
      
      res.writeHead(201, {'content-type': 'application/json'});
      res.end(JSON.stringify(POST_BODY));
    } else {
      res.writeHead(405, {'content-type': 'application/json'});
      res.end(JSON.stringify({message: 'Method not allowed'}));
    }
  });
}).listen(port, '127.0.0.1', () => console.log('READY', port));
