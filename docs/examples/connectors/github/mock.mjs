#!/usr/bin/env node
// Mock GitHub API server for admission testing
// Simulates /repos/{owner}/{repo}/events endpoint

import http from 'http';

const events = [
  {
    id: '34567890123',
    type: 'IssuesEvent',
    actor: {
      id: 12345678,
      login: 'octocat',
      display_login: 'octocat',
      url: 'https://api.github.com/users/octocat'
    },
    repo: {
      id: 98765432,
      name: 'octocat/Hello-World',
      url: 'https://api.github.com/repos/octocat/Hello-World'
    },
    payload: {
      action: 'opened',
      issue: {
        id: 1234567890,
        number: 1347,
        state: 'open',
        title: 'Found a bug',
        body: "I'm having a problem with this.",
        user: {
          login: 'octocat',
          id: 12345678
        },
        labels: [
          {
            name: 'bug',
            color: 'd73a4a'
          }
        ],
        created_at: '2023-11-23T12:00:00Z'
      }
    },
    public: true,
    created_at: '2023-11-23T12:00:00Z'
  },
  {
    id: '34567890124',
    type: 'PullRequestEvent',
    actor: {
      id: 12345679,
      login: 'contributor',
      display_login: 'contributor',
      url: 'https://api.github.com/users/contributor'
    },
    repo: {
      id: 98765432,
      name: 'octocat/Hello-World',
      url: 'https://api.github.com/repos/octocat/Hello-World'
    },
    payload: {
      action: 'opened',
      number: 42,
      pull_request: {
        id: 987654321,
        number: 42,
        state: 'open',
        title: 'Fix bug in authentication',
        user: {
          login: 'contributor',
          id: 12345679
        },
        created_at: '2023-11-23T12:05:00Z'
      }
    },
    public: true,
    created_at: '2023-11-23T12:05:00Z'
  }
];

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, X-GitHub-Api-Version, Accept, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  // GET /repos/{owner}/{repo}/events - list repository events
  if (req.method === 'GET' && url.pathname.match(/^\/repos\/[^\/]+\/[^\/]+\/events$/)) {
    const perPage = parseInt(url.searchParams.get('per_page') || '30');
    const page = parseInt(url.searchParams.get('page') || '1');
    const since = url.searchParams.get('since');
    
    let filteredEvents = events;
    if (since) {
      const sinceIndex = events.findIndex(e => e.id === since);
      filteredEvents = sinceIndex >= 0 ? events.slice(0, sinceIndex) : events;
    }
    
    const startIndex = (page - 1) * perPage;
    const data = filteredEvents.slice(startIndex, startIndex + perPage);
    
    // GitHub uses Link header for pagination
    const linkHeader = [];
    if (page > 1) {
      linkHeader.push(`<${url.pathname}?page=${page - 1}&per_page=${perPage}>; rel="prev"`);
    }
    if (startIndex + perPage < filteredEvents.length) {
      linkHeader.push(`<${url.pathname}?page=${page + 1}&per_page=${perPage}>; rel="next"`);
    }
    
    res.writeHead(200, { 
      'Content-Type': 'application/json',
      'Link': linkHeader.join(', ')
    });
    res.end(JSON.stringify(data));
    return;
  }

  // POST /repos/{owner}/{repo}/issues - create issue
  if (req.method === 'POST' && url.pathname.match(/^\/repos\/[^\/]+\/[^\/]+\/issues$/)) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      const data = JSON.parse(body);
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        id: Date.now(),
        number: Math.floor(Math.random() * 10000),
        state: 'open',
        title: data.title,
        body: data.body,
        user: {
          login: 'octocat',
          id: 12345678
        },
        created_at: new Date().toISOString()
      }));
    });
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    message: 'Not Found',
    documentation_url: 'https://docs.github.com/rest'
  }));
});

const PORT = process.env.PORT || 8081;
server.listen(PORT, () => {
  console.log(`Mock GitHub API listening on port ${PORT}`);
});
