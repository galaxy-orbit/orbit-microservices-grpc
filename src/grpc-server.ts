import { Server, Transport, type TransportOptions } from '@galaxy-stack/orbit-microservices';
import {
  createSecureServer,
  createServer,
  type Http2SecureServer,
  type Http2Server,
  type ServerHttp2Stream,
  type IncomingHttpHeaders,
  constants,
} from 'node:http2';
import { readFileSync } from 'node:fs';

const {
  HTTP2_HEADER_STATUS,
  HTTP2_HEADER_CONTENT_TYPE,
  HTTP2_HEADER_PATH,
  HTTP2_HEADER_METHOD,
} = constants;

export interface GrpcServerOptions extends TransportOptions {
  host?: string;
  port?: number;
  package?: string;
  protoPath?: string | string[];
  protoLoader?: {
    keepCase?: boolean;
    longs?: 'String' | 'Number';
    enums?: 'String' | 'Number';
    defaults?: boolean;
    oneofs?: boolean;
    includeDirs?: string[];
  };
  credentials?: {
    key: string | Buffer;
    cert: string | Buffer;
    ca?: string | Buffer;
  };
  insecure?: boolean;
  maxSendMessageLength?: number;
  maxReceiveMessageLength?: number;
}

interface GrpcMethod {
  service: string;
  method: string;
  handler: (data: any, metadata?: any) => Promise<any>;
  requestStream?: boolean;
  responseStream?: boolean;
}

const GRPC_STATUS = {
  OK: 0,
  CANCELLED: 1,
  UNKNOWN: 2,
  INVALID_ARGUMENT: 3,
  DEADLINE_EXCEEDED: 4,
  NOT_FOUND: 5,
  ALREADY_EXISTS: 6,
  PERMISSION_DENIED: 7,
  RESOURCE_EXHAUSTED: 8,
  FAILED_PRECONDITION: 9,
  ABORTED: 10,
  OUT_OF_RANGE: 11,
  UNIMPLEMENTED: 12,
  INTERNAL: 13,
  UNAVAILABLE: 14,
  DATA_LOSS: 15,
  UNAUTHENTICATED: 16,
} as const;

export class GrpcServer extends Server {
  private readonly options: GrpcServerOptions;
  private server: Http2SecureServer | Http2Server | null = null;
  private methods: Map<string, GrpcMethod> = new Map();
  private isListening = false;

  constructor(options: GrpcServerOptions = {}) {
    super();
    this.options = {
      host: options.host || '0.0.0.0',
      port: options.port ?? 50051,
      package: options.package || 'orbit',
      insecure: options.insecure ?? true,
      protoLoader: options.protoLoader || {
        keepCase: true,
        longs: 'String',
        enums: 'String',
        defaults: true,
        oneofs: true,
      },
      maxSendMessageLength: options.maxSendMessageLength || 4 * 1024 * 1024,
      maxReceiveMessageLength: options.maxReceiveMessageLength || 4 * 1024 * 1024,
      ...options,
    };
  }

  async listen(callback?: () => void): Promise<void> {
    try {
      await this.bindMethodHandlers();
      await this.startServer();
      this.isListening = true;
      callback?.();
    } catch (error) {
      console.error('[GrpcServer] Failed to start:', error);
      throw error;
    }
  }

  private async bindMethodHandlers(): Promise<void> {
    for (const [rawPattern, handler] of this.messageHandlers) {
      // Server normalizes object patterns to JSON strings — parse them back
      // so { service, method } registrations map to the right gRPC path.
      let pattern: string | object = rawPattern;
      if (typeof rawPattern === 'string' && rawPattern.startsWith('{')) {
        try {
          pattern = JSON.parse(rawPattern);
        } catch {
          // plain string pattern — leave as is
        }
      }

      const { service, method } = this.parsePattern(pattern);
      const fullPath = `/${this.options.package}.${service}/${method}`;
      
      this.methods.set(fullPath, {
        service,
        method,
        handler,
        requestStream: false,
        responseStream: false,
      });
    }
  }

  private parsePattern(pattern: string | object): { service: string; method: string } {
    if (typeof pattern === 'object') {
      const p = pattern as { service?: string; method?: string; rpc?: string };
      // GrpcMethod decorator emits { service, rpc }; keep both spellings working
      return { service: p.service ?? 'Default', method: p.method ?? p.rpc ?? 'Unknown' };
    }
    
    const parts = pattern.split('/').filter(Boolean);
    if (parts.length >= 2) {
      return { service: parts[0], method: parts[1] };
    }
    return { service: 'Default', method: pattern };
  }

  private async startServer(): Promise<void> {
    const { host, port, insecure, credentials } = this.options;

    if (insecure) {
      this.server = createServer();
    } else if (credentials) {
      const key = typeof credentials.key === 'string' 
        ? readFileSync(credentials.key) 
        : credentials.key;
      const cert = typeof credentials.cert === 'string' 
        ? readFileSync(credentials.cert) 
        : credentials.cert;
      
      this.server = createSecureServer({
        key,
        cert,
        ca: credentials.ca 
          ? (typeof credentials.ca === 'string' ? readFileSync(credentials.ca) : credentials.ca)
          : undefined,
        allowHTTP1: false,
      });
    } else {
      throw new Error('[GrpcServer] Either insecure:true or credentials must be provided');
    }

    this.server.on('stream', (stream, headers) => {
      this.handleStream(stream, headers).catch(err => {
        console.error('[GrpcServer] Stream error:', err);
        this.sendError(stream, GRPC_STATUS.INTERNAL, err.message || 'Internal error');
      });
    });

    this.server.on('error', (err) => {
      console.error('[GrpcServer] Server error:', err);
    });

    await new Promise<void>((resolve, reject) => {
      this.server!.listen(port!, host, () => {
        console.log(`[GrpcServer] Listening on ${insecure ? 'h2c' : 'h2'}://${host}:${port}`);
        resolve();
      });

      this.server!.once('error', reject);
    });
  }

  private async handleStream(
    stream: ServerHttp2Stream,
    headers: IncomingHttpHeaders
  ): Promise<void> {
    const path = headers[HTTP2_HEADER_PATH] as string;
    const method = headers[HTTP2_HEADER_METHOD] as string;
    const contentType = headers['content-type'] as string;

    if (method !== 'POST') {
      this.sendError(stream, GRPC_STATUS.UNIMPLEMENTED, 'Only POST method is supported');
      return;
    }

    if (!contentType?.startsWith('application/grpc')) {
      this.sendError(stream, GRPC_STATUS.INVALID_ARGUMENT, 'Invalid content-type');
      return;
    }

    const grpcMethod = this.methods.get(path);
    if (!grpcMethod) {
      this.sendError(stream, GRPC_STATUS.UNIMPLEMENTED, `Method not found: ${path}`);
      return;
    }

    const chunks: Buffer[] = [];
    
    stream.on('data', (chunk: Buffer) => {
      chunks.push(chunk);
    });

    stream.on('end', async () => {
      try {
        const body = Buffer.concat(chunks);
        const data = this.decodeMessage(body);
        
        const metadata = this.extractMetadata(headers);
        const result = await grpcMethod.handler(data, metadata);
        
        const encoded = this.encodeMessage(result);
        
        stream.on('wantTrailers', () => {
          stream.sendTrailers({
            'grpc-status': String(GRPC_STATUS.OK),
            'grpc-message': '',
          });
        });
        
        stream.respond({
          [HTTP2_HEADER_STATUS]: 200,
          [HTTP2_HEADER_CONTENT_TYPE]: 'application/grpc+json',
        }, { waitForTrailers: true });
        
        stream.write(encoded);
        stream.end();
        
      } catch (error: any) {
        console.error('[GrpcServer] Handler error:', error);
        this.sendError(stream, GRPC_STATUS.INTERNAL, error.message || 'Handler error');
      }
    });

    stream.on('error', (err) => {
      console.error('[GrpcServer] Stream error:', err);
    });
  }

  private extractMetadata(headers: IncomingHttpHeaders): Record<string, string> {
    const metadata: Record<string, string> = {};
    
    for (const [key, value] of Object.entries(headers)) {
      if (key.startsWith('grpc-') || key.startsWith('x-')) {
        metadata[key] = Array.isArray(value) ? value[0] : (value || '');
      }
    }
    
    return metadata;
  }

  private sendError(stream: ServerHttp2Stream, status: number, message: string): void {
    if (stream.destroyed || stream.closed) return;
    
    try {
      stream.on('wantTrailers', () => {
        stream.sendTrailers({
          'grpc-status': String(status),
          'grpc-message': encodeURIComponent(message),
        });
      });
      
      stream.respond({
        [HTTP2_HEADER_STATUS]: 200,
        [HTTP2_HEADER_CONTENT_TYPE]: 'application/grpc+json',
      }, { waitForTrailers: true, endStream: false });
      
      stream.end();
    } catch (err) {
      console.error('[GrpcServer] Failed to send error:', err);
    }
  }

  private decodeMessage(data: Buffer): any {
    if (data.length < 5) return {};
    
    const compressed = data[0];
    const messageLength = data.readUInt32BE(1);
    
    if (data.length < 5 + messageLength) return {};
    
    const messageData = data.subarray(5, 5 + messageLength);
    
    try {
      return JSON.parse(messageData.toString('utf8'));
    } catch {
      return { raw: messageData };
    }
  }

  private encodeMessage(data: any): Buffer {
    const json = JSON.stringify(data);
    const messageBytes = Buffer.from(json, 'utf8');
    const length = messageBytes.length;
    
    const result = Buffer.alloc(5 + length);
    result[0] = 0;
    result.writeUInt32BE(length, 1);
    messageBytes.copy(result, 5);
    
    return result;
  }

  async close(): Promise<void> {
    this.isListening = false;
    
    if (this.server) {
      await new Promise<void>((resolve) => {
        this.server!.close(() => {
          resolve();
        });
      });
      this.server = null;
    }
    
    this.methods.clear();
  }
}
