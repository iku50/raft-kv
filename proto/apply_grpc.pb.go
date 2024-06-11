// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.4.0
// - protoc             v5.26.1
// source: apply.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.62.0 or later.
const _ = grpc.SupportPackageIsVersion8

const (
	Apply_Apply_FullMethodName = "/raftpb.Apply/Apply"
)

// ApplyClient is the client API for Apply service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ApplyClient interface {
	Apply(ctx context.Context, in *ApplyArgs, opts ...grpc.CallOption) (*ApplyReply, error)
}

type applyClient struct {
	cc grpc.ClientConnInterface
}

func NewApplyClient(cc grpc.ClientConnInterface) ApplyClient {
	return &applyClient{cc}
}

func (c *applyClient) Apply(ctx context.Context, in *ApplyArgs, opts ...grpc.CallOption) (*ApplyReply, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ApplyReply)
	err := c.cc.Invoke(ctx, Apply_Apply_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ApplyServer is the server API for Apply service.
// All implementations must embed UnimplementedApplyServer
// for forward compatibility
type ApplyServer interface {
	Apply(context.Context, *ApplyArgs) (*ApplyReply, error)
	mustEmbedUnimplementedApplyServer()
}

// UnimplementedApplyServer must be embedded to have forward compatible implementations.
type UnimplementedApplyServer struct {
}

func (UnimplementedApplyServer) Apply(context.Context, *ApplyArgs) (*ApplyReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Apply not implemented")
}
func (UnimplementedApplyServer) mustEmbedUnimplementedApplyServer() {}

// UnsafeApplyServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ApplyServer will
// result in compilation errors.
type UnsafeApplyServer interface {
	mustEmbedUnimplementedApplyServer()
}

func RegisterApplyServer(s grpc.ServiceRegistrar, srv ApplyServer) {
	s.RegisterService(&Apply_ServiceDesc, srv)
}

func _Apply_Apply_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ApplyArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ApplyServer).Apply(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Apply_Apply_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ApplyServer).Apply(ctx, req.(*ApplyArgs))
	}
	return interceptor(ctx, in, info, handler)
}

// Apply_ServiceDesc is the grpc.ServiceDesc for Apply service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Apply_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raftpb.Apply",
	HandlerType: (*ApplyServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Apply",
			Handler:    _Apply_Apply_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "apply.proto",
}