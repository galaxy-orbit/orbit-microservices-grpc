import { describe, test, expect } from 'bun:test';
import { GrpcClient } from './grpc-client';

describe('GrpcClient (pattern parsing + message encoding)', () => {
  const client = new GrpcClient({ package: 'orbit.v1' });

  test('parsePattern: slash format', () => {
    const parsed = (client as any).parsePattern('users/Create');
    expect(parsed).toEqual({ service: 'users', method: 'Create' });
  });

  test('parsePattern: object form passes through', () => {
    const parsed = (client as any).parsePattern({ service: 'items', method: 'List' });
    expect(parsed).toEqual({ service: 'items', method: 'List' });
  });

  test('parsePattern: bare method falls back to Default service', () => {
    const parsed = (client as any).parsePattern('ping');
    expect(parsed).toEqual({ service: 'Default', method: 'ping' });
  });

  test('encodeMessage produces gRPC-framed JSON bytes', () => {
    const buf = (client as any).encodeMessage({ a: 1 });
    expect(buf[0]).toBe(0); // compressed flag
    expect(buf.readUInt32BE(1)).toBe(buf.length - 5);
    expect(JSON.parse(buf.subarray(5).toString())).toEqual({ a: 1 });
  });


});
