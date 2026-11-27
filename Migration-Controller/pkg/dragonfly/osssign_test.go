/*
Copyright (c) 2024-2026 Alibaba Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dragonfly

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/url"
	"testing"
	"time"
)

const (
	testAK     = "LTAI5tTestKeyId"
	testSecret = "test-access-key-secret"
	testURL    = "https://my-bucket.oss-cn-wulanchabu.aliyuncs.com/a/b/checkpoint.tar"
)

// TestSignOSSURLFormat verifies that the signed URL carries the three OSS V1
// signature parameters and that the signature matches the canonical
// StringToSign construction "GET\n\n\n{Expires}\n/{bucket}{object}".
func TestSignOSSURLFormat(t *testing.T) {
	signed, err := SignOSSURL(testURL, testAK, testSecret, time.Hour)
	if err != nil {
		t.Fatalf("SignOSSURL: %v", err)
	}

	u, err := url.Parse(signed)
	if err != nil {
		t.Fatalf("parse signed url: %v", err)
	}
	if u.Scheme != "https" || u.Host != "my-bucket.oss-cn-wulanchabu.aliyuncs.com" || u.Path != "/a/b/checkpoint.tar" {
		t.Fatalf("signed url changed scheme/host/path: %s", signed)
	}

	q := u.Query()
	if got := q.Get("OSSAccessKeyId"); got != testAK {
		t.Errorf("OSSAccessKeyId = %q, want %q", got, testAK)
	}
	expires := q.Get("Expires")
	if expires == "" {
		t.Fatal("Expires missing")
	}
	gotSig := q.Get("Signature")
	if gotSig == "" {
		t.Fatal("Signature missing")
	}

	stringToSign := "GET\n\n\n" + expires + "\n/my-bucket/a/b/checkpoint.tar"
	mac := hmac.New(sha1.New, []byte(testSecret))
	mac.Write([]byte(stringToSign))
	wantSig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if gotSig != wantSig {
		t.Errorf("Signature = %q, want %q (StringToSign %q)", gotSig, wantSig, stringToSign)
	}
}

// TestSignOSSURLRejects covers invalid inputs that would produce a broken
// signature.
func TestSignOSSURLRejects(t *testing.T) {
	cases := []struct {
		name, rawURL, ak, secret string
	}{
		{"missing ak", testURL, "", testSecret},
		{"missing secret", testURL, testAK, ""},
		{"non oss host", "https://example.com/a.tar", testAK, testSecret},
		{"existing query", testURL + "?foo=bar", testAK, testSecret},
		{"no object path", "https://my-bucket.oss-cn-wulanchabu.aliyuncs.com/", testAK, testSecret},
	}
	for _, c := range cases {
		if _, err := SignOSSURL(c.rawURL, c.ak, c.secret, time.Hour); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}

func TestIsOSSURL(t *testing.T) {
	if !IsOSSURL(testURL) {
		t.Error("expected OSS URL to be recognized")
	}
	if IsOSSURL("http://dragonfly-manager.dragonfly.svc:8080/health") {
		t.Error("non-OSS URL wrongly recognized")
	}
}

// TestSignedURLMapsToSameTaskID ensures the signature parameters are stripped
// by TaskIDFromURL, so the probe task id of the plain CR URL matches the
// dfdaemon task id of the signed download URL.
func TestSignedURLMapsToSameTaskID(t *testing.T) {
	signed, err := SignOSSURL(testURL, testAK, testSecret, time.Hour)
	if err != nil {
		t.Fatalf("SignOSSURL: %v", err)
	}
	if TaskIDFromURL(signed) != TaskIDFromURL(testURL) {
		t.Error("signed and plain URL must map to the same task id")
	}
}
