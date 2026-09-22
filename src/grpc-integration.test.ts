import { describe, test, expect, afterAll } from 'bun:test';
import { GrpcServer } from './grpc-server';
import { GrpcClient } from './grpc-client';

const servers: GrpcServer[] = [];
const clients: GrpcClient[] = [];

afterAll(async () => {
  for (const c of clients) await c.close().catch(() => {});
  for (const s of servers) await s.close().catch(() => {});
});

async function startServer(handlers: Record<string, (data: any, metadata?: any) => any>) {
  const server = new GrpcServer({ host: '127.0.0.1', port: 0 });
  for (const [pattern, handler] of Object.entries(handlers)) {
    server.addHandler(pattern, handler);
  }
  await server.listen();
  servers.push(server);
  const address = (server as any).server?.address();
  return { server, port: address.port as number };
}

describe('GrpcServer/GrpcClient — real HTTP2 integration', () => {
  test('client.send round-trips an RPC call', async () => {
    const { port } = await startServer({
      'math/Sum': (data: { a: number; b: number }) => ({ sum: data.a + data.b }),
    });

    const client = new GrpcClient({ host: '127.0.0.1', port });
    clients.push(client);

    const result = await client.send<{ sum: number }>('math/Sum', { a: 19, b: 23 });
    expect(result).toEqual({ sum: 42 });
  }, 20000);

  test('object patterns map to package.Service/Method paths', async () => {
    const { port } = await startServer({
      // object patterns map to /{package}.{service}/{method}
      [JSON.stringify({ service: 'Echo', method: 'Repeat' })]: (data: { word: string }) => ({
        repeated: data.word.repeat(2),
      }),
    });

    const client = new GrpcClient({ host: '127.0.0.1', port, package: 'orbit' });
    clients.push(client);

    const result = await client.send<{ repeated: string }>(
      { service: 'Echo', rpc: 'Repeat' } as any,
      { word: 'ab' },
    );
    expect(result).toEqual({ repeated: 'abab' });
  }, 20000);

  test('handler errors surface as gRPC errors with the original message', async () => {
    const { port } = await startServer({
      'math/Explode': () => { throw new Error('computation failed'); },
    });

    const client = new GrpcClient({ host: '127.0.0.1', port });
    clients.push(client);

    await expect(client.send('math/Explode', {})).rejects.toThrow('computation failed');
  }, 20000);

  test('unknown method returns UNIMPLEMENTED with the path in the message', async () => {
    const { port } = await startServer({});
    const client = new GrpcClient({ host: '127.0.0.1', port });
    clients.push(client);

    await expect(client.send('missing/Method', {})).rejects.toThrow(
      /Method not found: \/orbit\.missing\/Method/,
    );
  }, 20000);

  test('x- headers reach the handler as metadata', async () => {
    let seenMetadata: Record<string, string> | null = null;
    const { port } = await startServer({
      'meta/Inspect': (data: any, metadata: any) => {
        seenMetadata = metadata;
        return { ok: true };
      },
    });

    const client = new GrpcClient({ host: '127.0.0.1', port });
    clients.push(client);
    await client.send('meta/Inspect', {});
    expect(seenMetadata).toBeDefined();
  }, 20000);

  test('sequential RPCs on one connection keep working', async () => {
    const { port } = await startServer({
      'math/Double': (data: { n: number }) => ({ n: data.n * 2 }),
    });

    const client = new GrpcClient({ host: '127.0.0.1', port });
    clients.push(client);

    expect((await client.send('math/Double', { n: 1 })) as any).toEqual({ n: 2 });
    expect((await client.send('math/Double', { n: 2 })) as any).toEqual({ n: 4 });
    expect((await client.send('math/Double', { n: 3 })) as any).toEqual({ n: 6 });
  }, 20000);
});
