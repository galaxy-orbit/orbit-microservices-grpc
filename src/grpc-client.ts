import { ClientProxy, Transport, type ReadPacket } from '@galaxy-stack/orbit-microservices';
import {
  connect,
  type ClientHttp2Session,
  type ClientHttp2Stream,
  constants,
} from 'node:http2';
import { readFileSync } from 'node:fs';
import type { GrpcServerOptions } from './grpc-server';

const {
  HTTP2_HEADER_PATH,
  HTTP2_HEADER_METHOD,
  HTTP2_HEADER_CONTENT_TYPE,
  HTTP2_HEADER_STATUS,
} = constants;

export interface GrpcClientOptions extends GrpcServerOptions {
  keepalive?: {
    time?: number;
    timeout?: number;
    permitWithoutCalls?: boolean;
  };
  requestTimeout?: number;
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

export class GrpcClient extends ClientProxy {
  private readonly options: GrpcClientOptions;
  private session: ClientHttp2Session | null = null;
  protected override isConnected = false;
  private reconnecting = false;

  constructor(options: GrpcClientOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port ?? 50051,
      package: options.package || 'orbit',
      insecure: options.insecure ?? true,
      maxSendMessageLength: options.maxSendMessageLength || 4 * 1024 * 1024,
      maxReceiveMessageLength: options.maxReceiveMessageLength || 4 * 1024 * 1024,
      requestTimeout: options.requestTimeout || 30000,
      ...options,
    };
  }

  async connect(): Promise<void> {
    if (this.isConnected && this.session && !this.session.destroyed) {
      return;
    }

    const { host, port, insecure, credentials } = this.options;
    const protocol = insecure ? 'http' : 'https';
    const url = `${protocol}://${host}:${port}`;

    return new Promise((resolve, reject) => {
      const connectOptions: any = {};

      if (!insecure && credentials) {
        if (credentials.ca) {
          connectOptions.ca = typeof credentials.ca === 'string' 
            ? readFileSync(credentials.ca) 
            : credentials.ca;
        }
        if (credentials.key) {
          connectOptions.key = typeof credentials.key === 'string' 
            ? readFileSync(credentials.key) 
            : credentials.key;
        }
        if (credentials.cert) {
          connectOptions.cert = typeof credentials.cert === 'string' 
            ? readFileSync(credentials.cert) 
            : credentials.cert;
        }
      }

      this.session = connect(url, connectOptions);

      this.session.on('connect', () => {
        this.isConnected = true;
        console.log(`[GrpcClient] Connected to ${url}`);
        resolve();
      });

      this.session.on('error', (err) => {
        console.error('[GrpcClient] Session error:', err);
        if (!this.isConnected) {
          reject(err);
        }
      });

      this.session.on('close', () => {
        this.isConnected = false;
        this.session = null;
      });

      this.session.on('goaway', (errorCode, lastStreamID, opaqueData) => {
        console.log('[GrpcClient] Received GOAWAY:', errorCode);
      });

      setTimeout(() => {
        if (!this.isConnected) {
          reject(new Error('Connection timeout'));
        }
      }, 10000);
    });
  }

  protected async dispatchEvent(packet: ReadPacket): Promise<void> {
    await this.connect();
    
    const { service, method } = this.parsePattern(packet.pattern);
    const path = `/${this.options.package}.${service}/${method}`;
    
    const encoded = this.encodeMessage(packet.data);
    
    await this.sendRequest(path, encoded);
  }

  protected publish(packet: ReadPacket, callback: (packet: any) => void): () => void {
    this.connect()
      .then(() => {
        const { service, method } = this.parsePattern(packet.pattern);
        const path = `/${this.options.package}.${service}/${method}`;
        const encoded = this.encodeMessage(packet.data);
        
        return this.sendRequest(path, encoded);
      })
      .then((response) => {
        if (response.status !== GRPC_STATUS.OK) {
          callback({ err: response.message || `gRPC error: ${response.status}` });
        } else {
          callback({ response: response.data });
        }
      })
      .catch((err) => {
        callback({ err: err.message || 'Unknown error' });
      });

    return () => {};
  }

  send<TResult = any>(pattern: any, data: any): Promise<TResult> {
    return new Promise(async (resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Request timeout'));
      }, this.options.requestTimeout);

      try {
        await this.connect();

        const { service, method } = this.parsePattern(pattern);
        const path = `/${this.options.package}.${service}/${method}`;
        
        const encoded = this.encodeMessage(data);
        const response = await this.sendRequest(path, encoded);
        
        clearTimeout(timeout);
        
        if (response.status !== GRPC_STATUS.OK) {
          reject(new Error(response.message || `gRPC error: ${response.status}`));
          return;
        }

        resolve(response.data as TResult);
      } catch (error) {
        clearTimeout(timeout);
        reject(error);
      }
    });
  }

  private sendRequest(path: string, body: Buffer): Promise<{
    status: number;
    message: string;
    data: any;
  }> {
    return new Promise((resolve, reject) => {
      if (!this.session || this.session.destroyed) {
        reject(new Error('Session not connected'));
        return;
      }

      const req = this.session.request({
        [HTTP2_HEADER_PATH]: path,
        [HTTP2_HEADER_METHOD]: 'POST',
        [HTTP2_HEADER_CONTENT_TYPE]: 'application/grpc+json',
        'te': 'trailers',
      });

      const chunks: Buffer[] = [];
      let grpcStatus: number = GRPC_STATUS.OK;
      let grpcMessage = '';
      let responseHeaders: Record<string, string> = {};

      req.on('response', (headers) => {
        responseHeaders = headers as any;
        
        const statusHeader = headers['grpc-status'];
        if (statusHeader !== undefined) {
          grpcStatus = parseInt(String(statusHeader), 10);
        }
        
        const messageHeader = headers['grpc-message'];
        if (messageHeader) {
          grpcMessage = decodeURIComponent(String(messageHeader));
        }
      });

      req.on('data', (chunk: Buffer) => {
        chunks.push(chunk);
      });

      req.on('trailers', (trailers) => {
        const statusTrailer = trailers['grpc-status'];
        if (statusTrailer !== undefined) {
          grpcStatus = parseInt(String(statusTrailer), 10);
        }
        
        const messageTrailer = trailers['grpc-message'];
        if (messageTrailer) {
          grpcMessage = decodeURIComponent(String(messageTrailer));
        }
      });

      req.on('end', () => {
        const responseBody = Buffer.concat(chunks);
        const data = this.decodeMessage(responseBody);
        
        resolve({
          status: grpcStatus,
          message: grpcMessage,
          data,
        });
      });

      req.on('error', (err) => {
        reject(err);
      });

      req.write(body);
      req.end();
    });
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

  private decodeMessage(data: Buffer): any {
    if (data.length < 5) return {};
    
    const messageLength = data.readUInt32BE(1);
    
    if (data.length < 5 + messageLength) return {};
    
    const messageData = data.subarray(5, 5 + messageLength);
    
    try {
      return JSON.parse(messageData.toString('utf8'));
    } catch {
      return { raw: messageData };
    }
  }

  async close(): Promise<void> {
    this.isConnected = false;
    
    if (this.session && !this.session.destroyed) {
      await new Promise<void>((resolve) => {
        this.session!.close(() => {
          resolve();
        });
      });
    }
    
    this.session = null;
  }
}
