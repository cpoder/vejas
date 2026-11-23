#!/usr/bin/env node
// Mock Jira API server for admission testing
// Simulates /rest/api/3/search endpoint with JQL queries

import http from 'http';

const issues = [
  {
    id: '10001',
    key: 'PROJ-123',
    self: 'https://yourcompany.atlassian.net/rest/api/3/issue/10001',
    fields: {
      summary: 'Customer reported login issue',
      description: {
        type: 'doc',
        version: 1,
        content: [
          {
            type: 'paragraph',
            content: [
              {
                type: 'text',
                text: 'Users are unable to log in to the application.'
              }
            ]
          }
        ]
      },
      issuetype: {
        id: '10004',
        name: 'Bug',
        subtask: false
      },
      project: {
        id: '10000',
        key: 'PROJ',
        name: 'Project Name'
      },
      status: {
        id: '10001',
        name: 'To Do',
        statusCategory: {
          id: 2,
          key: 'new',
          name: 'To Do'
        }
      },
      priority: {
        id: '2',
        name: 'High'
      },
      assignee: {
        accountId: '5b10a2844c20165700ede21g',
        displayName: 'John Developer',
        emailAddress: 'developer@example.com'
      },
      reporter: {
        accountId: '5b10a2844c20165700ede21f',
        displayName: 'Support Team',
        emailAddress: 'support@example.com'
      },
      created: '2023-11-23T12:00:00.000+0000',
      updated: '2023-11-23T12:30:00.000+0000',
      labels: ['authentication', 'critical'],
      components: [
        {
          id: '10000',
          name: 'Authentication'
        }
      ]
    }
  },
  {
    id: '10002',
    key: 'PROJ-124',
    self: 'https://yourcompany.atlassian.net/rest/api/3/issue/10002',
    fields: {
      summary: 'Add dark mode support',
      issuetype: {
        id: '10001',
        name: 'Story',
        subtask: false
      },
      project: {
        id: '10000',
        key: 'PROJ',
        name: 'Project Name'
      },
      status: {
        id: '10002',
        name: 'In Progress'
      },
      priority: {
        id: '3',
        name: 'Medium'
      },
      created: '2023-11-23T11:00:00.000+0000',
      updated: '2023-11-23T12:15:00.000+0000'
    }
  }
];

const server = http.createServer((req, res) => {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Accept, Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(200);
    res.end();
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host}`);
  
  // GET /rest/api/3/search - JQL search
  if (req.method === 'GET' && url.pathname === '/rest/api/3/search') {
    const jql = url.searchParams.get('jql') || '';
    const startAt = parseInt(url.searchParams.get('startAt') || '0');
    const maxResults = parseInt(url.searchParams.get('maxResults') || '50');
    
    // Simple filtering based on JQL (just for demo)
    let filteredIssues = issues;
    if (jql.includes('updated >')) {
      // Return all issues for simplicity
      filteredIssues = issues;
    }
    
    const data = filteredIssues.slice(startAt, startAt + maxResults);
    
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      expand: 'names,schema',
      startAt,
      maxResults,
      total: filteredIssues.length,
      issues: data
    }));
    return;
  }

  // POST /rest/api/3/issue - create issue
  if (req.method === 'POST' && url.pathname === '/rest/api/3/issue') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      const data = JSON.parse(body);
      const newIssue = {
        id: String(Date.now()),
        key: `PROJ-${Math.floor(Math.random() * 1000)}`,
        self: `https://yourcompany.atlassian.net/rest/api/3/issue/${Date.now()}`
      };
      
      res.writeHead(201, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(newIssue));
    });
    return;
  }

  // GET /rest/api/3/issue/{issueIdOrKey} - get issue
  if (req.method === 'GET' && url.pathname.match(/^\/rest\/api\/3\/issue\/[^\/]+$/)) {
    const issueKey = url.pathname.split('/').pop();
    const issue = issues.find(i => i.key === issueKey || i.id === issueKey);
    
    if (issue) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(issue));
    } else {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        errorMessages: ['Issue does not exist or you do not have permission to see it.']
      }));
    }
    return;
  }

  // 404 for unknown endpoints
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    errorMessages: ['The requested resource was not found.']
  }));
});

const PORT = process.env.PORT || 8083;
server.listen(PORT, () => {
  console.log(`Mock Jira API listening on port ${PORT}`);
});
