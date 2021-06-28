// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package kvstore

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// KVStoreRPCClient is the client API for KVStoreRPC service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KVStoreRPCClient interface {
	BatchSubmit(ctx context.Context, in *BatchSubmitArgs, opts ...grpc.CallOption) (*BatchSubmitReply, error)
	BatchRead(ctx context.Context, in *BatchReadArgs, opts ...grpc.CallOption) (*BatchReadReply, error)
}

type kVStoreRPCClient struct {
	cc grpc.ClientConnInterface
}

func NewKVStoreRPCClient(cc grpc.ClientConnInterface) KVStoreRPCClient {
	return &kVStoreRPCClient{cc}
}

func (c *kVStoreRPCClient) BatchSubmit(ctx context.Context, in *BatchSubmitArgs, opts ...grpc.CallOption) (*BatchSubmitReply, error) {
	out := new(BatchSubmitReply)
	err := c.cc.Invoke(ctx, "/kvstore.KVStoreRPC/BatchSubmit", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *kVStoreRPCClient) BatchRead(ctx context.Context, in *BatchReadArgs, opts ...grpc.CallOption) (*BatchReadReply, error) {
	out := new(BatchReadReply)
	err := c.cc.Invoke(ctx, "/kvstore.KVStoreRPC/BatchRead", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KVStoreRPCServer is the server API for KVStoreRPC service.
// All implementations must embed UnimplementedKVStoreRPCServer
// for forward compatibility
type KVStoreRPCServer interface {
	BatchSubmit(context.Context, *BatchSubmitArgs) (*BatchSubmitReply, error)
	BatchRead(context.Context, *BatchReadArgs) (*BatchReadReply, error)
	mustEmbedUnimplementedKVStoreRPCServer()
}

// UnimplementedKVStoreRPCServer must be embedded to have forward compatible implementations.
type UnimplementedKVStoreRPCServer struct {
}

func (UnimplementedKVStoreRPCServer) BatchSubmit(context.Context, *BatchSubmitArgs) (*BatchSubmitReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BatchSubmit not implemented")
}
func (UnimplementedKVStoreRPCServer) BatchRead(context.Context, *BatchReadArgs) (*BatchReadReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method BatchRead not implemented")
}
func (UnimplementedKVStoreRPCServer) mustEmbedUnimplementedKVStoreRPCServer() {}

// UnsafeKVStoreRPCServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KVStoreRPCServer will
// result in compilation errors.
type UnsafeKVStoreRPCServer interface {
	mustEmbedUnimplementedKVStoreRPCServer()
}

func RegisterKVStoreRPCServer(s grpc.ServiceRegistrar, srv KVStoreRPCServer) {
	s.RegisterService(&KVStoreRPC_ServiceDesc, srv)
}

func _KVStoreRPC_BatchSubmit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchSubmitArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVStoreRPCServer).BatchSubmit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/kvstore.KVStoreRPC/BatchSubmit",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVStoreRPCServer).BatchSubmit(ctx, req.(*BatchSubmitArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _KVStoreRPC_BatchRead_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BatchReadArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVStoreRPCServer).BatchRead(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/kvstore.KVStoreRPC/BatchRead",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVStoreRPCServer).BatchRead(ctx, req.(*BatchReadArgs))
	}
	return interceptor(ctx, in, info, handler)
}

// KVStoreRPC_ServiceDesc is the grpc.ServiceDesc for KVStoreRPC service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KVStoreRPC_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "kvstore.KVStoreRPC",
	HandlerType: (*KVStoreRPCServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "BatchSubmit",
			Handler:    _KVStoreRPC_BatchSubmit_Handler,
		},
		{
			MethodName: "BatchRead",
			Handler:    _KVStoreRPC_BatchRead_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "kvstore.proto",
}