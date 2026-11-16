package __

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Removing the health additions must reproduce the original protocol descriptor.
// The digest covers messages, nested types, enums, field numbers, methods, and options.
func TestLegacyProtocolCompatibility(t *testing.T) {
	file := protodesc.ToFileDescriptorProto(File_api_outer_proto)
	messages := file.MessageType[:0]
	for _, message := range file.MessageType {
		switch message.GetName() {
		case "GetAllHealthRequest", "GetAllHealthResponse", "CategoryHealth", "GetCategoriesRequest", "GetCategoriesResponse", "CategoryInfo":
		default:
			messages = append(messages, message)
		}
	}
	file.MessageType = messages
	enums := file.EnumType[:0]
	for _, enum := range file.EnumType {
		if enum.GetName() != "HealthStatus" {
			enums = append(enums, enum)
		}
	}
	file.EnumType = enums
	for _, service := range file.Service {
		if service.GetName() != "XpuDeviceManagerService" {
			continue
		}
		methods := service.Method[:0]
		for _, method := range service.Method {
			if method.GetName() != "GetAllHealth" && method.GetName() != "GetCategories" {
				methods = append(methods, method)
			}
		}
		service.Method = methods
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	const originalDigest = "b5b2d31b63c7f667ceac227dd83ac078e2de4c48a91cfe4a485f6fd373029880"
	if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != originalDigest {
		t.Fatalf("legacy protocol descriptor changed: sha256=%s, want %s", got, originalDigest)
	}
}

// Check field numbers and types independently of the client and test server.
func TestHealthWireContract(t *testing.T) {
	type field struct {
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		target   protoreflect.FullName
	}
	messages := map[protoreflect.Name][]field{
		"GetAllHealthRequest": {},
		"GetAllHealthResponse": {
			{"node_name", 1, protoreflect.StringKind, false, ""},
			{"sampled_at_ms", 2, protoreflect.Int64Kind, false, ""},
			{"ttl_ms", 3, protoreflect.Int64Kind, false, ""},
			{"categories", 4, protoreflect.MessageKind, true, "CategoryHealth"},
		},
		"CategoryHealth": {
			{"category", 1, protoreflect.StringKind, false, ""},
			{"status", 2, protoreflect.EnumKind, false, "HealthStatus"},
			{"reason", 3, protoreflect.StringKind, false, ""},
		},
		"GetCategoriesRequest": {},
		"GetCategoriesResponse": {
			{"categories", 1, protoreflect.MessageKind, true, "CategoryInfo"},
		},
		"CategoryInfo": {
			{"category", 1, protoreflect.StringKind, false, ""},
			{"condition", 2, protoreflect.StringKind, false, ""},
			{"ok_reason", 3, protoreflect.StringKind, false, ""},
			{"ok_message", 4, protoreflect.StringKind, false, ""},
			{"bad_reason", 5, protoreflect.StringKind, false, ""},
		},
	}
	for name, fields := range messages {
		message := File_api_outer_proto.Messages().ByName(name)
		if message == nil || message.Fields().Len() != len(fields) {
			t.Fatalf("%s fields do not match the wire contract", name)
		}
		for _, want := range fields {
			got := message.Fields().ByName(want.name)
			if got == nil || got.Number() != want.number || got.Kind() != want.kind || got.IsList() != want.repeated {
				t.Fatalf("%s.%s number or type does not match the wire contract", name, want.name)
			}
			if want.kind == protoreflect.MessageKind && got.Message().FullName() != want.target {
				t.Fatalf("%s.%s references an unexpected message type", name, want.name)
			}
			if want.kind == protoreflect.EnumKind && got.Enum().FullName() != want.target {
				t.Fatalf("%s.%s references an unexpected enum type", name, want.name)
			}
		}
	}
	states := []protoreflect.Name{"HEALTH_STATUS_UNSPECIFIED", "HEALTHY", "UNHEALTHY", "UNKNOWN"}
	enum := File_api_outer_proto.Enums().ByName("HealthStatus")
	if enum == nil || enum.Values().Len() != len(states) {
		t.Fatal("health status enum does not match the wire contract")
	}
	for number, name := range states {
		value := enum.Values().ByName(name)
		if value == nil || value.Number() != protoreflect.EnumNumber(number) {
			t.Fatalf("health status %s value does not match the wire contract", name)
		}
	}
	service := File_api_outer_proto.Services().ByName("XpuDeviceManagerService")
	if service == nil {
		t.Fatal("XDM service is missing")
	}
	for _, name := range []protoreflect.Name{"GetAllHealth", "GetCategories"} {
		method := service.Methods().ByName(name)
		if method == nil || method.Input().Name() != name+"Request" || method.Output().Name() != name+"Response" || method.IsStreamingClient() || method.IsStreamingServer() {
			t.Fatalf("%s request, response, or streaming mode does not match the wire contract", name)
		}
	}
}
