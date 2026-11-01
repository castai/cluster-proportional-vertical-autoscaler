/*
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

package k8sclient

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/version"
)

// discoveryServer serves /version and /api/v1. The first `failures` /version
// calls (or all of them, with failures < 0) return 500.
func discoveryServer(t *testing.T, failures int32, gitVersion string, hasResize bool) (*httptest.Server, *int32) {
	t.Helper()
	var calls int32
	writeJSON := func(w http.ResponseWriter, obj interface{}) {
		b, _ := json.Marshal(obj)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/version":
			n := atomic.AddInt32(&calls, 1)
			if failures < 0 || n <= failures {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			writeJSON(w, &version.Info{GitVersion: gitVersion})
		case "/api/v1":
			resources := []metav1.APIResource{{Name: "pods", Namespaced: true, Kind: "Pod"}}
			if hasResize {
				resources = append(resources, metav1.APIResource{Name: "pods/resize", Namespaced: true, Kind: "Pod"})
			}
			writeJSON(w, &metav1.APIResourceList{GroupVersion: "v1", APIResources: resources})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server, &calls
}

// TestResolveResizeMode covers F5: a transient discovery failure must not lock
// InPlaceOrRecreate into Recreate; only a definitive "unsupported" answer
// degrades it, and that answer is not retried.
func TestResolveResizeMode(t *testing.T) {
	fast := wait.Backoff{Duration: time.Millisecond, Factor: 1, Steps: 4}

	for _, tc := range []struct {
		name         string
		mode         ResizeMode
		failures     int32 // -1: always fail
		gitVersion   string
		hasResize    bool
		wantMode     ResizeMode
		wantErr      bool
		wantErrIs    error
		wantAttempts int32
	}{
		{name: "transient errors then supported", mode: ResizeModeInPlaceOrRecreate, failures: 2, gitVersion: "v1.34.0", hasResize: true,
			wantMode: ResizeModeInPlaceOrRecreate, wantAttempts: 3},
		{name: "discovery keeps failing: error, not degraded", mode: ResizeModeInPlaceOrRecreate, failures: -1, gitVersion: "v1.34.0", hasResize: true,
			wantErr: true, wantAttempts: 4},
		{name: "no subresource: degraded without retrying", mode: ResizeModeInPlaceOrRecreate, gitVersion: "v1.34.0", hasResize: false,
			wantMode: ResizeModeRecreate, wantAttempts: 1},
		{name: "1.32 server: degraded", mode: ResizeModeInPlaceOrRecreate, gitVersion: "v1.32.4-gke.100", hasResize: true,
			wantMode: ResizeModeRecreate, wantAttempts: 1},
		{name: "InPlace on unsupported cluster fails fast", mode: ResizeModeInPlace, gitVersion: "v1.34.0", hasResize: false,
			wantErr: true, wantErrIs: ErrResizeUnsupported, wantAttempts: 1},
		{name: "InPlace survives transient errors", mode: ResizeModeInPlace, failures: 1, gitVersion: "v1.33.1", hasResize: true,
			wantMode: ResizeModeInPlace, wantAttempts: 2},
		{name: "Recreate skips discovery", mode: ResizeModeRecreate, failures: -1,
			wantMode: ResizeModeRecreate, wantAttempts: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, calls := discoveryServer(t, tc.failures, tc.gitVersion, tc.hasResize)
			got, err := resolveResizeMode(newResizeTestClient(server), tc.mode, fast)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("mode=%s err=nil, want an error", got)
				}
				if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
					t.Errorf("err = %v, want errors.Is %v", err, tc.wantErrIs)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got != tc.wantMode {
					t.Errorf("mode = %s, want %s", got, tc.wantMode)
				}
			}
			if n := atomic.LoadInt32(calls); n != tc.wantAttempts {
				t.Errorf("/version calls = %d, want %d", n, tc.wantAttempts)
			}
		})
	}
}
