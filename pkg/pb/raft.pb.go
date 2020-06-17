// Compile: protoc --go_out=plugins=grpc:./pkg/pb pkg/pb/raft.proto

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.11.2
// source: pkg/pb/raft.proto

package pb

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type RequestVoteReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term         uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	CandidateID  int32  `protobuf:"varint,2,opt,name=candidateID,proto3" json:"candidateID,omitempty"`
	LastLogIndex uint64 `protobuf:"varint,3,opt,name=lastLogIndex,proto3" json:"lastLogIndex,omitempty"`
	LastLogTerm  uint64 `protobuf:"varint,4,opt,name=lastLogTerm,proto3" json:"lastLogTerm,omitempty"`
}

func (x *RequestVoteReq) Reset() {
	*x = RequestVoteReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_raft_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteReq) ProtoMessage() {}

func (x *RequestVoteReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_raft_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteReq.ProtoReflect.Descriptor instead.
func (*RequestVoteReq) Descriptor() ([]byte, []int) {
	return file_pkg_pb_raft_proto_rawDescGZIP(), []int{0}
}

func (x *RequestVoteReq) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteReq) GetCandidateID() int32 {
	if x != nil {
		return x.CandidateID
	}
	return 0
}

func (x *RequestVoteReq) GetLastLogIndex() uint64 {
	if x != nil {
		return x.LastLogIndex
	}
	return 0
}

func (x *RequestVoteReq) GetLastLogTerm() uint64 {
	if x != nil {
		return x.LastLogTerm
	}
	return 0
}

type RequestVoteResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Term        uint64 `protobuf:"varint,1,opt,name=term,proto3" json:"term,omitempty"`
	VoteGranted bool   `protobuf:"varint,2,opt,name=voteGranted,proto3" json:"voteGranted,omitempty"`
}

func (x *RequestVoteResp) Reset() {
	*x = RequestVoteResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_raft_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RequestVoteResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RequestVoteResp) ProtoMessage() {}

func (x *RequestVoteResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_raft_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RequestVoteResp.ProtoReflect.Descriptor instead.
func (*RequestVoteResp) Descriptor() ([]byte, []int) {
	return file_pkg_pb_raft_proto_rawDescGZIP(), []int{1}
}

func (x *RequestVoteResp) GetTerm() uint64 {
	if x != nil {
		return x.Term
	}
	return 0
}

func (x *RequestVoteResp) GetVoteGranted() bool {
	if x != nil {
		return x.VoteGranted
	}
	return false
}

type AppendEntriesReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AppendEntriesReq) Reset() {
	*x = AppendEntriesReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_raft_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesReq) ProtoMessage() {}

func (x *AppendEntriesReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_raft_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesReq.ProtoReflect.Descriptor instead.
func (*AppendEntriesReq) Descriptor() ([]byte, []int) {
	return file_pkg_pb_raft_proto_rawDescGZIP(), []int{2}
}

type AppendEntriesResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AppendEntriesResp) Reset() {
	*x = AppendEntriesResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_raft_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AppendEntriesResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AppendEntriesResp) ProtoMessage() {}

func (x *AppendEntriesResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_raft_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AppendEntriesResp.ProtoReflect.Descriptor instead.
func (*AppendEntriesResp) Descriptor() ([]byte, []int) {
	return file_pkg_pb_raft_proto_rawDescGZIP(), []int{3}
}

var File_pkg_pb_raft_proto protoreflect.FileDescriptor

var file_pkg_pb_raft_proto_rawDesc = []byte{
	0x0a, 0x11, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x62, 0x2f, 0x72, 0x61, 0x66, 0x74, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0x8c, 0x01, 0x0a, 0x0e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56,
	0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x63, 0x61,
	0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x44, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x0b, 0x63, 0x61, 0x6e, 0x64, 0x69, 0x64, 0x61, 0x74, 0x65, 0x49, 0x44, 0x12, 0x22, 0x0a, 0x0c,
	0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x0c, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x49, 0x6e, 0x64, 0x65, 0x78,
	0x12, 0x20, 0x0a, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65, 0x72, 0x6d, 0x18,
	0x04, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x6c, 0x61, 0x73, 0x74, 0x4c, 0x6f, 0x67, 0x54, 0x65,
	0x72, 0x6d, 0x22, 0x47, 0x0a, 0x0f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x04, 0x74, 0x65, 0x72, 0x6d, 0x12, 0x20, 0x0a, 0x0b, 0x76, 0x6f, 0x74,
	0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0b,
	0x76, 0x6f, 0x74, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x22, 0x12, 0x0a, 0x10, 0x41,
	0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x22,
	0x13, 0x0a, 0x11, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x32, 0x74, 0x0a, 0x04, 0x52, 0x61, 0x66, 0x74, 0x12, 0x32, 0x0a, 0x0b,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x12, 0x0f, 0x2e, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x71, 0x1a, 0x10, 0x2e, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x56, 0x6f, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00,
	0x12, 0x38, 0x0a, 0x0d, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65,
	0x73, 0x12, 0x11, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74, 0x72, 0x69, 0x65,
	0x73, 0x52, 0x65, 0x71, 0x1a, 0x12, 0x2e, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x45, 0x6e, 0x74,
	0x72, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x42, 0x24, 0x5a, 0x22, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63, 0x68, 0x75, 0x79, 0x61, 0x6e, 0x67,
	0x6c, 0x69, 0x75, 0x2f, 0x72, 0x61, 0x77, 0x6b, 0x76, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x62,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_pb_raft_proto_rawDescOnce sync.Once
	file_pkg_pb_raft_proto_rawDescData = file_pkg_pb_raft_proto_rawDesc
)

func file_pkg_pb_raft_proto_rawDescGZIP() []byte {
	file_pkg_pb_raft_proto_rawDescOnce.Do(func() {
		file_pkg_pb_raft_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_pb_raft_proto_rawDescData)
	})
	return file_pkg_pb_raft_proto_rawDescData
}

var file_pkg_pb_raft_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_pkg_pb_raft_proto_goTypes = []interface{}{
	(*RequestVoteReq)(nil),    // 0: RequestVoteReq
	(*RequestVoteResp)(nil),   // 1: RequestVoteResp
	(*AppendEntriesReq)(nil),  // 2: AppendEntriesReq
	(*AppendEntriesResp)(nil), // 3: AppendEntriesResp
}
var file_pkg_pb_raft_proto_depIdxs = []int32{
	0, // 0: Raft.RequestVote:input_type -> RequestVoteReq
	2, // 1: Raft.AppendEntries:input_type -> AppendEntriesReq
	1, // 2: Raft.RequestVote:output_type -> RequestVoteResp
	3, // 3: Raft.AppendEntries:output_type -> AppendEntriesResp
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_pb_raft_proto_init() }
func file_pkg_pb_raft_proto_init() {
	if File_pkg_pb_raft_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_pb_raft_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_pb_raft_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RequestVoteResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_pb_raft_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesReq); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_pkg_pb_raft_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AppendEntriesResp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_pb_raft_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_pb_raft_proto_goTypes,
		DependencyIndexes: file_pkg_pb_raft_proto_depIdxs,
		MessageInfos:      file_pkg_pb_raft_proto_msgTypes,
	}.Build()
	File_pkg_pb_raft_proto = out.File
	file_pkg_pb_raft_proto_rawDesc = nil
	file_pkg_pb_raft_proto_goTypes = nil
	file_pkg_pb_raft_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// RaftClient is the client API for Raft service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type RaftClient interface {
	// RequestVote invoked by candidates to gather votes.
	RequestVote(ctx context.Context, in *RequestVoteReq, opts ...grpc.CallOption) (*RequestVoteResp, error)
	// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
	AppendEntries(ctx context.Context, in *AppendEntriesReq, opts ...grpc.CallOption) (*AppendEntriesResp, error)
}

type raftClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftClient(cc grpc.ClientConnInterface) RaftClient {
	return &raftClient{cc}
}

func (c *raftClient) RequestVote(ctx context.Context, in *RequestVoteReq, opts ...grpc.CallOption) (*RequestVoteResp, error) {
	out := new(RequestVoteResp)
	err := c.cc.Invoke(ctx, "/Raft/RequestVote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftClient) AppendEntries(ctx context.Context, in *AppendEntriesReq, opts ...grpc.CallOption) (*AppendEntriesResp, error) {
	out := new(AppendEntriesResp)
	err := c.cc.Invoke(ctx, "/Raft/AppendEntries", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServer is the server API for Raft service.
type RaftServer interface {
	// RequestVote invoked by candidates to gather votes.
	RequestVote(context.Context, *RequestVoteReq) (*RequestVoteResp, error)
	// AppendEntries invoked by leader to replicate log entries, also used as heartbeat.
	AppendEntries(context.Context, *AppendEntriesReq) (*AppendEntriesResp, error)
}

// UnimplementedRaftServer can be embedded to have forward compatible implementations.
type UnimplementedRaftServer struct {
}

func (*UnimplementedRaftServer) RequestVote(context.Context, *RequestVoteReq) (*RequestVoteResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (*UnimplementedRaftServer) AppendEntries(context.Context, *AppendEntriesReq) (*AppendEntriesResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}

func RegisterRaftServer(s *grpc.Server, srv RaftServer) {
	s.RegisterService(&_Raft_serviceDesc, srv)
}

func _Raft_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Raft/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).RequestVote(ctx, req.(*RequestVoteReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Raft_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Raft/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServer).AppendEntries(ctx, req.(*AppendEntriesReq))
	}
	return interceptor(ctx, in, info, handler)
}

var _Raft_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Raft",
	HandlerType: (*RaftServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _Raft_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _Raft_AppendEntries_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/pb/raft.proto",
}
