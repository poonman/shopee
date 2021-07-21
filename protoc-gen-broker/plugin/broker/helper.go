package broker

import (
	"github.com/poonman/shopee/protoc-gen-broker/api"
	"github.com/gogo/protobuf/proto"
	google_protobuf "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
)

func GetServiceRule(field *google_protobuf.ServiceDescriptorProto) *api.ServiceRule {
	if field == nil {
		return nil
	}
	if field.Options != nil {
		v, err := proto.GetExtension(field.Options, api.E_Service)
		if err == nil && v.(*api.ServiceRule) != nil {
			return v.(*api.ServiceRule)
		}
	}
	return nil
}

func GetHttpRule(field *google_protobuf.MethodDescriptorProto) *api.HttpRule {
	if field == nil {
		return nil
	}
	if field.Options != nil {
		v, err := proto.GetExtension(field.Options, api.E_Http)
		if err == nil && v.(*api.HttpRule) != nil {
			return v.(*api.HttpRule)
		}
	}
	return nil
}
