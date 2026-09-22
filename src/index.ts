import { registerTransport, Transport } from '@galaxy-stack/orbit-microservices';
import { GrpcServer } from './grpc-server';
import { GrpcClient } from './grpc-client';

registerTransport(Transport.GRPC, GrpcServer, GrpcClient);

export { GrpcServer, type GrpcServerOptions } from './grpc-server';
export { GrpcClient, type GrpcClientOptions } from './grpc-client';
