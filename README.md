# @galaxy-stack/orbit-microservices-grpc

[![npm version](https://img.shields.io/npm/v/@galaxy-stack/orbit-microservices-grpc.svg)](https://www.npmjs.com/package/@galaxy-stack/orbit-microservices-grpc)
[![docs](https://img.shields.io/badge/docs-galaxy--orbit--framework.vercel.app-blue)](https://galaxy-orbit-framework.vercel.app)

Part of the [Orbit framework](https://github.com/galaxy-orbit/packages) — a NestJS-style backend framework for [Bun](https://bun.sh).

## Installation

```bash
bun add @galaxy-stack/orbit-microservices-grpc
```

# @galaxy-stack/orbit-microservices-grpc

> **Status**: ✅ Full Implementation - Complete gRPC over HTTP/2 với node:http2

## Mô tả

gRPC transport implementation cho Orbit microservices sử dụng HTTP/2 qua `node:http2` module.

## Tính năng

- Full HTTP/2 support qua `node:http2`
- gRPC message framing (length-prefixed)
- JSON serialization (protobuf support planned)
- Unary RPC pattern
- TLS/SSL support với certificates
- Insecure mode cho development
- gRPC status codes và trailers
- Metadata extraction từ headers
- Request timeout handling
- Graceful shutdown

## Cài đặt

```bash
bun add @galaxy-stack/orbit-microservices-grpc
```

## Sử dụng

### gRPC Server

```typescript
import { BunFactory } from '@galaxy-stack/orbit-core';
import '@galaxy-stack/orbit-microservices-grpc';

// Insecure mode (development)
const app = await BunFactory.createMicroservice(AppModule, {
  transport: 'GRPC',
  options: {
    host: '0.0.0.0',
    port: 50051,
    package: 'myservice',
    insecure: true,
  },
});

await app.listen();
// [GrpcServer] Listening on h2c://0.0.0.0:50051

// Secure mode (production)
const secureApp = await BunFactory.createMicroservice(AppModule, {
  transport: 'GRPC',
  options: {
    host: '0.0.0.0',
    port: 50051,
    package: 'myservice',
    insecure: false,
    credentials: {
      key: './certs/server.key',
      cert: './certs/server.crt',
      ca: './certs/ca.crt', // optional
    },
  },
});
```

### gRPC Client

```typescript
import { MicroservicesModule } from '@galaxy-stack/orbit-microservices';
import '@galaxy-stack/orbit-microservices-grpc';

@Module({
  imports: [
    MicroservicesModule.register({
      name: 'GRPC_SERVICE',
      transport: 'GRPC',
      options: {
        host: 'localhost',
        port: 50051,
        package: 'myservice',
        insecure: true,
      },
    }),
  ],
})
class ClientModule {}
```

### Message Handlers

```typescript
import { Controller, GrpcMethod } from '@galaxy-stack/orbit-microservices';

interface GetUserRequest {
  id: number;
}

interface User {
  id: number;
  name: string;
  email: string;
}

@Controller()
class UserController {
  @GrpcMethod('UserService', 'GetUser')
  getUser(data: GetUserRequest, metadata?: Record<string, string>): User {
    return { 
      id: data.id, 
      name: 'John Doe',
      email: 'john@example.com',
    };
  }

  @GrpcMethod('UserService', 'CreateUser')
  createUser(data: { name: string; email: string }): User {
    return { 
      id: Date.now(), 
      name: data.name,
      email: data.email,
    };
  }
}
```

### Client Usage

```typescript
@Injectable()
class UserService {
  constructor(
    @Inject('GRPC_SERVICE') private client: ClientProxy
  ) {}

  async getUser(id: number): Promise<User> {
    return this.client.send('UserService/GetUser', { id });
  }

  async createUser(name: string, email: string): Promise<User> {
    return this.client.send('UserService/CreateUser', { name, email });
  }
}
```

## Options

### GrpcServerOptions

```typescript
interface GrpcServerOptions {
  host?: string;                    // Default: '0.0.0.0'
  port?: number;                    // Default: 50051
  package?: string;                 // Package name (default: 'orbit')
  insecure?: boolean;               // Use h2c (HTTP/2 cleartext) (default: true)
  credentials?: {
    key: string | Buffer;           // Private key path or buffer
    cert: string | Buffer;          // Certificate path or buffer
    ca?: string | Buffer;           // CA certificate (optional)
  };
  protoPath?: string | string[];    // Proto file paths (future use)
  protoLoader?: {
    keepCase?: boolean;
    longs?: 'String' | 'Number';
    enums?: 'String' | 'Number';
    defaults?: boolean;
    oneofs?: boolean;
    includeDirs?: string[];
  };
  maxSendMessageLength?: number;    // Default: 4MB
  maxReceiveMessageLength?: number; // Default: 4MB
}
```

### GrpcClientOptions

```typescript
interface GrpcClientOptions extends GrpcServerOptions {
  requestTimeout?: number;          // Default: 30000ms
  keepalive?: {
    time?: number;
    timeout?: number;
    permitWithoutCalls?: boolean;
  };
}
```

## Pattern Format

gRPC patterns support:

- String: `"UserService/GetUser"`
- Object: `{ service: 'UserService', method: 'GetUser' }`

## Path Mapping

Path được xây dựng theo gRPC format:

```
/{package}.{service}/{method}
```

Ví dụ: `/myservice.UserService/GetUser`

## Message Framing

gRPC sử dụng length-prefixed framing:

```
┌────────────────┬────────────────┬────────────────────────┐
│ Compressed (1B)│ Length (4B BE) │ Message (N bytes)      │
└────────────────┴────────────────┴────────────────────────┘
```

## gRPC Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0 | OK | Success |
| 1 | CANCELLED | Operation cancelled |
| 2 | UNKNOWN | Unknown error |
| 3 | INVALID_ARGUMENT | Invalid argument |
| 4 | DEADLINE_EXCEEDED | Timeout |
| 5 | NOT_FOUND | Not found |
| 6 | ALREADY_EXISTS | Already exists |
| 7 | PERMISSION_DENIED | Permission denied |
| 8 | RESOURCE_EXHAUSTED | Resource exhausted |
| 9 | FAILED_PRECONDITION | Failed precondition |
| 10 | ABORTED | Aborted |
| 11 | OUT_OF_RANGE | Out of range |
| 12 | UNIMPLEMENTED | Not implemented |
| 13 | INTERNAL | Internal error |
| 14 | UNAVAILABLE | Service unavailable |
| 15 | DATA_LOSS | Data loss |
| 16 | UNAUTHENTICATED | Unauthenticated |

## Metadata

Headers với prefix `grpc-` và `x-` được extract thành metadata:

```typescript
@GrpcMethod('UserService', 'GetUser')
getUser(data: GetUserRequest, metadata?: Record<string, string>): User {
  console.log('Auth token:', metadata?.['x-auth-token']);
  return { id: data.id, name: 'John' };
}
```

## Error Handling

```typescript
@GrpcMethod('UserService', 'GetUser')
getUser(data: { id: number }) {
  const user = this.userRepo.find(data.id);
  if (!user) {
    throw new Error('User not found');
  }
  return user;
}

// Client side
try {
  const user = await client.send('UserService/GetUser', { id: 999 });
} catch (error) {
  console.error(error.message); // 'User not found'
}
```

## HTTP/2 Features

Full HTTP/2 support qua `node:http2`:

- Multiplexing - multiple requests trên single connection
- Header compression (HPACK)
- Stream prioritization
- Flow control
- Trailers cho gRPC status

## Insecure vs Secure Mode

| Mode | Protocol | Use Case |
|------|----------|----------|
| `insecure: true` | h2c (HTTP/2 cleartext) | Development, internal services |
| `insecure: false` | h2 (HTTP/2 over TLS) | Production, public APIs |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────────────────────────────────────────────────┤
│      GrpcServer          │         GrpcClient          │
├─────────────────────────────────────────────────────────┤
│              gRPC Message Framing                       │
├─────────────────────────────────────────────────────────┤
│              node:http2 (HTTP/2)                        │
└─────────────────────────────────────────────────────────┘
```

## Roadmap

- [ ] Protobuf serialization (currently JSON)
- [ ] Bidirectional streaming
- [ ] gRPC reflection
- [ ] Load balancing
- [ ] Health checking protocol
