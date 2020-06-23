// Compile: protoc --go_out=plugins=grpc:./pkg/pb pkg/pb/storage.proto

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.24.0
// 	protoc        v3.12.3
// source: pkg/pb/storage.proto

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

type GetReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *GetReq) Reset() {
	*x = GetReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetReq) ProtoMessage() {}

func (x *GetReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetReq.ProtoReflect.Descriptor instead.
func (*GetReq) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{0}
}

func (x *GetReq) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type GetResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Val   []byte `protobuf:"bytes,1,opt,name=val,proto3" json:"val,omitempty"`
	Found bool   `protobuf:"varint,2,opt,name=found,proto3" json:"found,omitempty"`
}

func (x *GetResp) Reset() {
	*x = GetResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResp) ProtoMessage() {}

func (x *GetResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetResp.ProtoReflect.Descriptor instead.
func (*GetResp) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{1}
}

func (x *GetResp) GetVal() []byte {
	if x != nil {
		return x.Val
	}
	return nil
}

func (x *GetResp) GetFound() bool {
	if x != nil {
		return x.Found
	}
	return false
}

type PutReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Val []byte `protobuf:"bytes,2,opt,name=val,proto3" json:"val,omitempty"`
}

func (x *PutReq) Reset() {
	*x = PutReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutReq) ProtoMessage() {}

func (x *PutReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutReq.ProtoReflect.Descriptor instead.
func (*PutReq) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{2}
}

func (x *PutReq) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *PutReq) GetVal() []byte {
	if x != nil {
		return x.Val
	}
	return nil
}

type PutResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *PutResp) Reset() {
	*x = PutResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResp) ProtoMessage() {}

func (x *PutResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutResp.ProtoReflect.Descriptor instead.
func (*PutResp) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{3}
}

type DelReq struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *DelReq) Reset() {
	*x = DelReq{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DelReq) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DelReq) ProtoMessage() {}

func (x *DelReq) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DelReq.ProtoReflect.Descriptor instead.
func (*DelReq) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{4}
}

func (x *DelReq) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

type DelResp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *DelResp) Reset() {
	*x = DelResp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_pkg_pb_storage_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DelResp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DelResp) ProtoMessage() {}

func (x *DelResp) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_pb_storage_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DelResp.ProtoReflect.Descriptor instead.
func (*DelResp) Descriptor() ([]byte, []int) {
	return file_pkg_pb_storage_proto_rawDescGZIP(), []int{5}
}

var File_pkg_pb_storage_proto protoreflect.FileDescriptor

var file_pkg_pb_storage_proto_rawDesc = []byte{
	0x0a, 0x14, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x62, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x1a, 0x0a, 0x06, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x22, 0x31, 0x0a, 0x07, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x12, 0x10, 0x0a,
	0x03, 0x76, 0x61, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x76, 0x61, 0x6c, 0x12,
	0x14, 0x0a, 0x05, 0x66, 0x6f, 0x75, 0x6e, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05,
	0x66, 0x6f, 0x75, 0x6e, 0x64, 0x22, 0x2c, 0x0a, 0x06, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x76, 0x61, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03,
	0x76, 0x61, 0x6c, 0x22, 0x09, 0x0a, 0x07, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x1a,
	0x0a, 0x06, 0x44, 0x65, 0x6c, 0x52, 0x65, 0x71, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22, 0x09, 0x0a, 0x07, 0x44, 0x65,
	0x6c, 0x52, 0x65, 0x73, 0x70, 0x32, 0x5d, 0x0a, 0x07, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65,
	0x12, 0x1a, 0x0a, 0x03, 0x47, 0x65, 0x74, 0x12, 0x07, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71,
	0x1a, 0x08, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x1a, 0x0a, 0x03,
	0x50, 0x75, 0x74, 0x12, 0x07, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x1a, 0x08, 0x2e, 0x50,
	0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x22, 0x00, 0x12, 0x1a, 0x0a, 0x03, 0x44, 0x65, 0x6c, 0x12,
	0x07, 0x2e, 0x44, 0x65, 0x6c, 0x52, 0x65, 0x71, 0x1a, 0x08, 0x2e, 0x44, 0x65, 0x6c, 0x52, 0x65,
	0x73, 0x70, 0x22, 0x00, 0x42, 0x24, 0x5a, 0x22, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x63, 0x68, 0x75, 0x79, 0x61, 0x6e, 0x67, 0x6c, 0x69, 0x75, 0x2f, 0x72, 0x61,
	0x77, 0x6b, 0x76, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var (
	file_pkg_pb_storage_proto_rawDescOnce sync.Once
	file_pkg_pb_storage_proto_rawDescData = file_pkg_pb_storage_proto_rawDesc
)

func file_pkg_pb_storage_proto_rawDescGZIP() []byte {
	file_pkg_pb_storage_proto_rawDescOnce.Do(func() {
		file_pkg_pb_storage_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_pb_storage_proto_rawDescData)
	})
	return file_pkg_pb_storage_proto_rawDescData
}

var file_pkg_pb_storage_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_pkg_pb_storage_proto_goTypes = []interface{}{
	(*GetReq)(nil),  // 0: GetReq
	(*GetResp)(nil), // 1: GetResp
	(*PutReq)(nil),  // 2: PutReq
	(*PutResp)(nil), // 3: PutResp
	(*DelReq)(nil),  // 4: DelReq
	(*DelResp)(nil), // 5: DelResp
}
var file_pkg_pb_storage_proto_depIdxs = []int32{
	0, // 0: Storage.Get:input_type -> GetReq
	2, // 1: Storage.Put:input_type -> PutReq
	4, // 2: Storage.Del:input_type -> DelReq
	1, // 3: Storage.Get:output_type -> GetResp
	3, // 4: Storage.Put:output_type -> PutResp
	5, // 5: Storage.Del:output_type -> DelResp
	3, // [3:6] is the sub-list for method output_type
	0, // [0:3] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_pb_storage_proto_init() }
func file_pkg_pb_storage_proto_init() {
	if File_pkg_pb_storage_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_pkg_pb_storage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetReq); i {
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
		file_pkg_pb_storage_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetResp); i {
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
		file_pkg_pb_storage_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutReq); i {
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
		file_pkg_pb_storage_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutResp); i {
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
		file_pkg_pb_storage_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DelReq); i {
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
		file_pkg_pb_storage_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DelResp); i {
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
			RawDescriptor: file_pkg_pb_storage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_pb_storage_proto_goTypes,
		DependencyIndexes: file_pkg_pb_storage_proto_depIdxs,
		MessageInfos:      file_pkg_pb_storage_proto_msgTypes,
	}.Build()
	File_pkg_pb_storage_proto = out.File
	file_pkg_pb_storage_proto_rawDesc = nil
	file_pkg_pb_storage_proto_goTypes = nil
	file_pkg_pb_storage_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// StorageClient is the client API for Storage service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type StorageClient interface {
	// Get returns the value associated with the key, and a boolean indicating whether the key exists.
	Get(ctx context.Context, in *GetReq, opts ...grpc.CallOption) (*GetResp, error)
	// Put adds or updates a key-value pair to the storage.
	Put(ctx context.Context, in *PutReq, opts ...grpc.CallOption) (*PutResp, error)
	// Del removes key from the storage.
	Del(ctx context.Context, in *DelReq, opts ...grpc.CallOption) (*DelResp, error)
}

type storageClient struct {
	cc grpc.ClientConnInterface
}

func NewStorageClient(cc grpc.ClientConnInterface) StorageClient {
	return &storageClient{cc}
}

func (c *storageClient) Get(ctx context.Context, in *GetReq, opts ...grpc.CallOption) (*GetResp, error) {
	out := new(GetResp)
	err := c.cc.Invoke(ctx, "/Storage/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storageClient) Put(ctx context.Context, in *PutReq, opts ...grpc.CallOption) (*PutResp, error) {
	out := new(PutResp)
	err := c.cc.Invoke(ctx, "/Storage/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storageClient) Del(ctx context.Context, in *DelReq, opts ...grpc.CallOption) (*DelResp, error) {
	out := new(DelResp)
	err := c.cc.Invoke(ctx, "/Storage/Del", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StorageServer is the server API for Storage service.
type StorageServer interface {
	// Get returns the value associated with the key, and a boolean indicating whether the key exists.
	Get(context.Context, *GetReq) (*GetResp, error)
	// Put adds or updates a key-value pair to the storage.
	Put(context.Context, *PutReq) (*PutResp, error)
	// Del removes key from the storage.
	Del(context.Context, *DelReq) (*DelResp, error)
}

// UnimplementedStorageServer can be embedded to have forward compatible implementations.
type UnimplementedStorageServer struct {
}

func (*UnimplementedStorageServer) Get(context.Context, *GetReq) (*GetResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (*UnimplementedStorageServer) Put(context.Context, *PutReq) (*PutResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (*UnimplementedStorageServer) Del(context.Context, *DelReq) (*DelResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Del not implemented")
}

func RegisterStorageServer(s *grpc.Server, srv StorageServer) {
	s.RegisterService(&_Storage_serviceDesc, srv)
}

func _Storage_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StorageServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Storage/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StorageServer).Get(ctx, req.(*GetReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Storage_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StorageServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Storage/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StorageServer).Put(ctx, req.(*PutReq))
	}
	return interceptor(ctx, in, info, handler)
}

func _Storage_Del_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DelReq)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StorageServer).Del(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Storage/Del",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StorageServer).Del(ctx, req.(*DelReq))
	}
	return interceptor(ctx, in, info, handler)
}

var _Storage_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Storage",
	HandlerType: (*StorageServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _Storage_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _Storage_Put_Handler,
		},
		{
			MethodName: "Del",
			Handler:    _Storage_Del_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/pb/storage.proto",
}
