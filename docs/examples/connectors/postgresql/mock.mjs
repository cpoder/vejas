#!/usr/bin/env node
// Mock PostgreSQL exec-rpc wrapper for admission testing
// Simulates vejas-postgresql binary that accepts JSON requests on stdin

import readline from 'readline';

const users = [
  {
    id: 1,
    name: 'John Doe',
    email: 'john.doe@example.com',
    created_at: '2023-11-23T12:00:00.000Z',
    updated_at: '2023-11-23T12:00:00.000Z',
    active: true,
    role: 'admin'
  },
  {
    id: 2,
    name: 'Jane Smith',
    email: 'jane.smith@example.com',
    created_at: '2023-11-23T11:30:00.000Z',
    updated_at: '2023-11-23T11:30:00.000Z',
    active: true,
    role: 'user'
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
    
    if (request.op === 'query') {
      // Simple SELECT query simulation
      if (request.sql.toLowerCase().includes('select')) {
        const response = {
          ok: true,
          rows: users,
          rowCount: users.length,
          command: 'SELECT',
          fields: [
            { name: 'id', dataTypeID: 23, dataType: 'int4' },
            { name: 'name', dataTypeID: 1043, dataType: 'varchar' },
            { name: 'email', dataTypeID: 1043, dataType: 'varchar' },
            { name: 'created_at', dataTypeID: 1184, dataType: 'timestamptz' },
            { name: 'updated_at', dataTypeID: 1184, dataType: 'timestamptz' },
            { name: 'active', dataTypeID: 16, dataType: 'bool' },
            { name: 'role', dataTypeID: 1043, dataType: 'varchar' }
          ]
        };
        console.log(JSON.stringify(response));
      } else {
        console.log(JSON.stringify({
          ok: false,
          error: 'Only SELECT queries are supported in mock'
        }));
      }
    } else if (request.op === 'execute') {
      // INSERT/UPDATE/DELETE simulation
      const response = {
        ok: true,
        rowCount: 1,
        command: request.sql.split(' ')[0].toUpperCase()
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'transaction') {
      // Transaction simulation
      const response = {
        ok: true,
        results: request.queries.map(() => ({ ok: true, rowCount: 1 }))
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

console.error('Mock PostgreSQL connector started');
