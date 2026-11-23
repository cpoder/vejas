#!/usr/bin/env node
// Mock Elasticsearch exec-rpc wrapper for admission testing
// Simulates vejas-elasticsearch binary that accepts JSON requests on stdin

import readline from 'readline';

const users = [
  {
    _index: 'users',
    _id: '1',
    _score: 1.0,
    _source: {
      name: 'John Doe',
      email: 'john.doe@example.com',
      age: 32,
      role: 'admin',
      active: true,
      created_at: '2023-11-23T12:00:00.000Z'
    }
  },
  {
    _index: 'users',
    _id: '2',
    _score: 0.8,
    _source: {
      name: 'Jane Smith',
      email: 'jane.smith@example.com',
      age: 28,
      role: 'user',
      active: true,
      created_at: '2023-11-23T11:30:00.000Z'
    }
  }
];

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false
});

rl.on('line', (line) => {
  try {
    const request = JSON.parse(line);
    
    if (request.op === 'search') {
      // Search documents
      const response = {
        ok: true,
        took: 5,
        timed_out: false,
        _shards: {
          total: 1,
          successful: 1,
          skipped: 0,
          failed: 0
        },
        hits: {
          total: {
            value: users.length,
            relation: 'eq'
          },
          max_score: 1.0,
          hits: users
        }
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'index') {
      // Index a document
      const response = {
        ok: true,
        _index: request.index,
        _id: request.id || Date.now().toString(),
        _version: 1,
        result: 'created'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'get') {
      // Get a document
      const response = {
        ok: true,
        _index: request.index,
        _id: request.id,
        _version: 1,
        found: true,
        _source: users[0]._source
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'update') {
      // Update a document
      const response = {
        ok: true,
        _index: request.index,
        _id: request.id,
        _version: 2,
        result: 'updated'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'delete') {
      // Delete a document
      const response = {
        ok: true,
        _index: request.index,
        _id: request.id,
        _version: 1,
        result: 'deleted'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'bulk') {
      // Bulk operations
      const response = {
        ok: true,
        took: 10,
        errors: false,
        items: [
          { index: { _index: 'users', _id: '1', _version: 1, result: 'created', status: 201 } },
          { index: { _index: 'users', _id: '2', _version: 1, result: 'created', status: 201 } }
        ]
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'aggregate') {
      // Aggregations
      const response = {
        ok: true,
        took: 5,
        hits: {
          total: { value: 2 },
          hits: []
        },
        aggregations: {
          total_sales: {
            value: 1500.50
          }
        }
      };
      console.log(JSON.stringify(response));
    } else {
      console.log(JSON.stringify({
        ok: false,
        error: `Unknown operation: ${request.op}`
      }));
    }
  } catch (err) {
    console.log(JSON.stringify({
      ok: false,
      error: err.message
    }));
  }
});

console.error('Mock Elasticsearch connector started');
