#!/usr/bin/env node
// Mock MongoDB exec-rpc wrapper for admission testing
// Simulates vejas-mongodb binary that accepts JSON requests on stdin

import readline from 'readline';

const users = [
  {
    _id: '507f1f77bcf86cd799439011',
    name: 'John Doe',
    email: 'john.doe@example.com',
    age: 32,
    role: 'admin',
    active: true,
    createdAt: '2023-11-23T12:00:00.000Z',
    updatedAt: '2023-11-23T12:00:00.000Z',
    tags: ['premium', 'verified'],
    address: {
      street: '123 Main St',
      city: 'San Francisco',
      state: 'CA',
      zip: '94103'
    }
  },
  {
    _id: '507f1f77bcf86cd799439012',
    name: 'Jane Smith',
    email: 'jane.smith@example.com',
    age: 28,
    role: 'user',
    active: true,
    createdAt: '2023-11-23T11:30:00.000Z',
    updatedAt: '2023-11-23T11:30:00.000Z',
    tags: ['verified'],
    address: {
      street: '456 Oak Ave',
      city: 'New York',
      state: 'NY',
      zip: '10001'
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
    
    if (request.op === 'find') {
      // Find documents
      const response = {
        ok: true,
        documents: users,
        count: users.length
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'findOne') {
      // Find one document
      const response = {
        ok: true,
        document: users[0]
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'insertOne') {
      // Insert one document
      const response = {
        ok: true,
        insertedId: '507f1f77bcf86cd799439013'
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'insertMany') {
      // Insert many documents
      const response = {
        ok: true,
        insertedIds: ['507f1f77bcf86cd799439013', '507f1f77bcf86cd799439014']
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'updateOne' || request.op === 'updateMany') {
      // Update documents
      const response = {
        ok: true,
        matchedCount: 1,
        modifiedCount: 1
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'deleteOne' || request.op === 'deleteMany') {
      // Delete documents
      const response = {
        ok: true,
        deletedCount: 1
      };
      console.log(JSON.stringify(response));
    } else if (request.op === 'aggregate') {
      // Aggregation pipeline
      const response = {
        ok: true,
        documents: [
          { _id: 'admin', count: 1 },
          { _id: 'user', count: 1 }
        ]
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

console.error('Mock MongoDB connector started');
