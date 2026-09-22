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

package autoscaler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/cluster-proportional-vertical-autoscaler/pkg/autoscaler/k8sclient"
	clocktesting "k8s.io/utils/clock/testing"
)

const testNamespace = "default"

// These are controller-level integration tests: they stand up a mocked API
// server, build a real k8sClient + AutoScaler, run pollAPIServer, and assert on
// the API calls that result. They cover the wiring (discovery -> NewK8sClient ->
// pollAPIServer -> the right mutating calls per mode); detailed classification,
// tracker and patch-building behaviour is unit-tested in the k8sclient package.

// ---------------------------------------------------------------------------
// requestRecorder: thread-safe capture of the mutating API calls made during a
// poll. httptest serves each request on its own goroutine, so the mutex is
// required for -race correctness (it also gives the test goroutine a clean
// happens-before on the recorded data once pollAPIServer returns).
// ---------------------------------------------------------------------------

type recordedPatch struct {
	Path string
	Body []byte
}

type requestRecorder struct {
	mu              sync.Mutex
	templatePatches []recordedPatch
	resizePatches   []recordedPatch
	deletes         []string
	evictions       []string
}

func (r *requestRecorder) recordTemplatePatch(path string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.templatePatches = append(r.templatePatches, recordedPatch{Path: path, Body: body})
}

func (r *requestRecorder) recordResizePatch(path string, body []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resizePatches = append(r.resizePatches, recordedPatch{Path: path, Body: body})
}

func (r *requestRecorder) recordDelete(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletes = append(r.deletes, path)
}

func (r *requestRecorder) recordEviction(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictions = append(r.evictions, path)
}

func (r *requestRecorder) templatePatched() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.templatePatches) > 0
}

func (r *requestRecorder) resizePatched() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.resizePatches) > 0
}

func (r *requestRecorder) deleteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deletes)
}

func (r *requestRecorder) evictionCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.evictions)
}

func (r *requestRecorder) lastTemplatePatch() (recordedPatch, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.templatePatches) == 0 {
		return recordedPatch{}, false
	}
	return r.templatePatches[len(r.templatePatches)-1], true
}

func (r *requestRecorder) resizePatchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.resizePatches)
}

// resizedPodNames returns the pod names extracted from the recorded /resize
// patch paths (.../pods/<name>/resize).
func (r *requestRecorder) resizedPodNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.resizePatches))
	for _, p := range r.resizePatches {
		trimmed := strings.TrimSuffix(p.Path, "/resize")
		names = append(names, trimmed[strings.LastIndex(trimmed, "/")+1:])
	}
	return names
}

// ---------------------------------------------------------------------------
// Typed patch assertions: replace interface{} type-assertion ladders.
// ---------------------------------------------------------------------------

type containerPatch struct {
	Name      string                     `json:"name"`
	Resources apiv1.ResourceRequirements `json:"resources"`
}

// templatePatchContainers extracts spec.template.spec.containers from a workload
// template patch body.
func templatePatchContainers(t *testing.T, body []byte) []containerPatch {
	t.Helper()
	var p struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []containerPatch `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("unmarshal template patch: %v\nbody: %s", err, body)
	}
	return p.Spec.Template.Spec.Containers
}

// cpuRequest returns the CPU request of the named container, or "" if absent.
func cpuRequest(ctrs []containerPatch, name string) string {
	for _, c := range ctrs {
		if c.Name == name {
			return c.Resources.Requests.Cpu().String()
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// fakeCluster: declarative description of the mocked cluster state.
// ---------------------------------------------------------------------------

type fakeCluster struct {
	cores     []int          // CPU capacity (cores) per node
	target    runtime.Object // workload object served on GET and patched on PATCH
	pods      []apiv1.Pod    // pods returned by the pod LIST
	hasResize bool           // whether discovery advertises the pods/resize subresource
}

func ctr(name, cpu string) apiv1.Container {
	return apiv1.Container{
		Name: name,
		Resources: apiv1.ResourceRequirements{
			Requests: apiv1.ResourceList{apiv1.ResourceCPU: resource.MustParse(cpu)},
		},
	}
}

// targetUID is the deterministic UID assigned to a built workload, so a pod's
// controller ownerRef can reference it (DaemonSet/StatefulSet/ReplicaSet
// ownership is matched by UID).
func targetUID(name string) types.UID { return types.UID(name + "-uid") }

func podTemplateSpec(containers ...apiv1.Container) apiv1.PodTemplateSpec {
	return apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
		Spec:       apiv1.PodSpec{Containers: containers},
	}
}

func deployment(name string, containers ...apiv1.Container) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: targetUID(name)},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: podTemplateSpec(containers...),
		},
	}
}

func daemonset(name string, strategy appsv1.DaemonSetUpdateStrategyType, containers ...apiv1.Container) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: targetUID(name)},
		Spec: appsv1.DaemonSetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: strategy},
			Template:       podTemplateSpec(containers...),
		},
	}
}

func statefulset(name string, strategy appsv1.StatefulSetUpdateStrategyType, containers ...apiv1.Container) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: targetUID(name)},
		Spec: appsv1.StatefulSetSpec{
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: strategy},
			Template:       podTemplateSpec(containers...),
		},
	}
}

func replicaset(name string, containers ...apiv1.Container) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, UID: targetUID(name)},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: podTemplateSpec(containers...),
		},
	}
}

func controllerRef(kind, name string) metav1.OwnerReference {
	controller := true
	return metav1.OwnerReference{Kind: kind, Name: name, Controller: &controller}
}

// ownedPod builds a Running pod controlled by the given ReplicaSet and labelled
// so it matches the workload selector.
func ownedPod(name, rsName string, container apiv1.Container, conds ...apiv1.PodCondition) apiv1.Pod {
	return apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testNamespace,
			UID:             types.UID(name),
			Labels:          map[string]string{"app": "test"},
			OwnerReferences: []metav1.OwnerReference{controllerRef("ReplicaSet", rsName)},
		},
		Spec:   apiv1.PodSpec{Containers: []apiv1.Container{container}},
		Status: apiv1.PodStatus{Phase: apiv1.PodRunning, Conditions: conds},
	}
}

// directlyOwnedPod builds a Running pod whose controller ownerRef points at the
// given workload (DaemonSet/StatefulSet/ReplicaSet ownership is matched by UID).
func directlyOwnedPod(t *testing.T, name string, target runtime.Object, container apiv1.Container, conds ...apiv1.PodCondition) apiv1.Pod {
	t.Helper()
	kind, _, tname := targetInfo(t, target)
	controller := true
	return apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			UID:       types.UID(name),
			Labels:    map[string]string{"app": "test"},
			OwnerReferences: []metav1.OwnerReference{{
				Kind:       kind,
				Name:       tname,
				UID:        targetUID(tname),
				Controller: &controller,
			}},
		},
		Spec:   apiv1.PodSpec{Containers: []apiv1.Container{container}},
		Status: apiv1.PodStatus{Phase: apiv1.PodRunning, Conditions: conds},
	}
}

// targetInfo returns (kind, resourcePlural, name) for the workload object.
func targetInfo(t *testing.T, obj runtime.Object) (kind, resourcePlural, name string) {
	t.Helper()
	switch o := obj.(type) {
	case *appsv1.Deployment:
		return "Deployment", "deployments", o.Name
	case *appsv1.DaemonSet:
		return "DaemonSet", "daemonsets", o.Name
	case *appsv1.StatefulSet:
		return "StatefulSet", "statefulsets", o.Name
	case *appsv1.ReplicaSet:
		return "ReplicaSet", "replicasets", o.Name
	default:
		t.Fatalf("unsupported target type %T", obj)
		return "", "", ""
	}
}

func (fc fakeCluster) targetRef(t *testing.T) string {
	kind, _, name := targetInfo(t, fc.target)
	return strings.ToLower(kind) + "/" + name
}

// handler builds the mock API server handler from the cluster state and routes
// mutating calls into rec.
func (fc fakeCluster) handler(t *testing.T, rec *requestRecorder) http.HandlerFunc {
	_, resourcePlural, name := targetInfo(t, fc.target)
	targetPath := "/apis/apps/v1/namespaces/" + testNamespace + "/" + resourcePlural + "/" + name
	podsPath := "/api/v1/namespaces/" + testNamespace + "/pods"

	writeJSON := func(w http.ResponseWriter, code int, obj interface{}) {
		b, _ := json.Marshal(obj)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		w.Write(b)
	}

	return func(w http.ResponseWriter, req *http.Request) {
		t.Logf("mock %s %s", req.Method, req.URL.Path)

		// --- discovery ---
		switch req.URL.Path {
		case "/api":
			writeJSON(w, http.StatusOK, &metav1.APIVersions{Versions: []string{"v1"}})
			return
		case "/api/v1":
			resources := []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"list", "get", "patch", "delete"}},
				{Name: "pods/eviction", Namespaced: true, Kind: "Eviction", Verbs: []string{"create"}},
				{Name: "nodes", Namespaced: false, Kind: "Node", Verbs: []string{"list"}},
			}
			if fc.hasResize {
				resources = append(resources, metav1.APIResource{Name: "pods/resize", Namespaced: true, Kind: "Pod", Verbs: []string{"patch"}})
			}
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{GroupVersion: "v1", APIResources: resources})
			return
		case "/apis":
			writeJSON(w, http.StatusOK, &metav1.APIGroupList{Groups: []metav1.APIGroup{{
				Name:             "apps",
				Versions:         []metav1.GroupVersionForDiscovery{{GroupVersion: "apps/v1", Version: "v1"}},
				PreferredVersion: metav1.GroupVersionForDiscovery{GroupVersion: "apps/v1", Version: "v1"},
			}}})
			return
		case "/apis/apps/v1":
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment"},
					{Name: "daemonsets", Namespaced: true, Kind: "DaemonSet"},
					{Name: "statefulsets", Namespaced: true, Kind: "StatefulSet"},
					{Name: "replicasets", Namespaced: true, Kind: "ReplicaSet"},
				},
			})
			return
		}

		// --- nodes list (cluster size) ---
		if req.Method == http.MethodGet && req.URL.Path == "/api/v1/nodes" {
			items := make([]apiv1.Node, 0, len(fc.cores))
			for _, c := range fc.cores {
				items = append(items, apiv1.Node{Status: apiv1.NodeStatus{
					Capacity: apiv1.ResourceList{apiv1.ResourceCPU: *resource.NewQuantity(int64(c), resource.DecimalSI)},
				}})
			}
			writeJSON(w, http.StatusOK, &apiv1.NodeList{Items: items})
			return
		}

		// --- target GET / template PATCH ---
		if req.URL.Path == targetPath {
			switch req.Method {
			case http.MethodGet:
				writeJSON(w, http.StatusOK, fc.target)
			case http.MethodPatch:
				body, _ := io.ReadAll(req.Body)
				rec.recordTemplatePatch(req.URL.Path, body)
				writeJSON(w, http.StatusOK, fc.target)
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}

		// --- pod LIST ---
		if req.Method == http.MethodGet && req.URL.Path == podsPath {
			writeJSON(w, http.StatusOK, &apiv1.PodList{Items: fc.pods})
			return
		}

		// --- pod resize / eviction / delete ---
		if strings.HasPrefix(req.URL.Path, podsPath+"/") {
			switch {
			case req.Method == http.MethodPatch && strings.HasSuffix(req.URL.Path, "/resize"):
				body, _ := io.ReadAll(req.Body)
				rec.recordResizePatch(req.URL.Path, body)
				writeJSON(w, http.StatusOK, struct{}{})
				return
			case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/eviction"):
				rec.recordEviction(req.URL.Path)
				writeJSON(w, http.StatusCreated, struct{}{})
				return
			case req.Method == http.MethodDelete:
				rec.recordDelete(req.URL.Path)
				writeJSON(w, http.StatusOK, struct{}{})
				return
			}
		}

		t.Logf("mock 404 %s %s", req.Method, req.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

// ---------------------------------------------------------------------------
// scenario + runPoll: one-call harness that absorbs all the setup boilerplate.
// ---------------------------------------------------------------------------

type scenario struct {
	cluster  fakeCluster
	mode     k8sclient.ResizeMode
	fallback k8sclient.ResizeFallbackConfig
	config   string // ScaleConfig JSON
	dryRun   bool
	cycles   int           // number of pollAPIServer calls; defaults to 1
	advance  time.Duration // resizer-clock step applied before each cycle after the first
}

// runPoll stands up the mock server, builds a real k8sClient + AutoScaler from
// the scenario, runs pollAPIServer the requested number of cycles, and returns
// the recorder plus the last poll error.
func runPoll(t *testing.T, s scenario) (*requestRecorder, error) {
	t.Helper()
	if s.cycles == 0 {
		s.cycles = 1
	}

	rec := &requestRecorder{}
	server := httptest.NewServer(s.cluster.handler(t, rec))
	defer server.Close()

	// The resizer clock drives the fallback grace period. A fake clock makes
	// multi-cycle fallback deterministic: a stuck pod is tracked on the first
	// cycle, then the clock is advanced past the grace period before the next.
	resizerClock := clocktesting.NewFakeClock(time.Now())

	kubeconfig := writeTempKubeconfig(t, server.URL)
	client, err := k8sclient.NewK8sClient(testNamespace, s.cluster.targetRef(t), kubeconfig, s.dryRun, s.mode, s.fallback, resizerClock)
	if err != nil {
		t.Fatalf("NewK8sClient: %v", err)
	}

	cfg := ScaleConfig{}
	if err := json.Unmarshal([]byte(s.config), &cfg); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}

	as := &AutoScaler{
		k8sClient:     client,
		defaultConfig: cfg,
		pollPeriod:    5 * time.Second,
		clock:         clocktesting.NewFakeClock(time.Now()),
		stopCh:        make(chan struct{}),
	}

	var pollErr error
	for i := 0; i < s.cycles; i++ {
		if i > 0 && s.advance > 0 {
			resizerClock.Step(s.advance)
		}
		if pollErr = as.pollAPIServer(context.Background()); pollErr != nil {
			break
		}
	}
	return rec, pollErr
}

// writeTempKubeconfig writes a minimal kubeconfig that points at the mock server
// so NewK8sClient can be initialised outside a cluster.
func writeTempKubeconfig(t *testing.T, serverURL string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	content := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: ` + serverURL + `
    insecure-skip-tls-verify: true
  name: mock
contexts:
- context:
    cluster: mock
  name: mock
current-context: mock
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPollAPIServer(t *testing.T) {
	// base 10m + step 1m per core; 4 nodes / 7 cores -> 17m desired.
	const baseConfig = `{"main":{"requests":{"cpu":{"base":"10m","step":"1m","coresPerStep":1}}}}`
	standardCores := []int{2, 2, 2, 1}

	standardCluster := func() fakeCluster {
		return fakeCluster{
			cores:     standardCores,
			target:    deployment("test-dep", ctr("main", "10m")),
			pods:      []apiv1.Pod{ownedPod("test-dep-abc-def", "test-dep-abc", ctr("main", "10m"))},
			hasResize: true,
		}
	}

	// In-place resize of a pod owned directly by a non-Deployment workload; used
	// to exercise selector/UID/ownership wiring per target kind.
	dsTarget := daemonset("test-ds", appsv1.RollingUpdateDaemonSetStrategyType, ctr("main", "10m"))
	ssTarget := statefulset("test-ss", appsv1.RollingUpdateStatefulSetStrategyType, ctr("main", "10m"))
	rsTarget := replicaset("test-rs", ctr("main", "10m"))

	kindCluster := func(target runtime.Object, podName string) fakeCluster {
		return fakeCluster{
			cores:     standardCores,
			target:    target,
			pods:      []apiv1.Pod{directlyOwnedPod(t, podName, target, ctr("main", "10m"))},
			hasResize: true,
		}
	}

	assertResizedNoTemplate := func(t *testing.T, rec *requestRecorder) {
		if !rec.resizePatched() {
			t.Fatal("pod was not resized via /resize")
		}
		if rec.templatePatched() {
			t.Error("template was patched unexpectedly in InPlace mode")
		}
	}

	cases := []struct {
		name     string
		scenario scenario
		assert   func(t *testing.T, rec *requestRecorder)
	}{
		{
			name:     "recreate mode patches the template with computed resources",
			scenario: scenario{cluster: standardCluster(), mode: k8sclient.ResizeModeRecreate, config: baseConfig},
			assert: func(t *testing.T, rec *requestRecorder) {
				if !rec.templatePatched() {
					t.Fatal("deployment template was NOT patched in Recreate mode")
				}
				if rec.resizePatched() {
					t.Error("unexpected /resize call in Recreate mode")
				}
				p, _ := rec.lastTemplatePatch()
				ctrs := templatePatchContainers(t, p.Body)
				if len(ctrs) != 1 || ctrs[0].Name != "main" {
					t.Fatalf("expected patch for container [main], got %+v", ctrs)
				}
				if cpu := cpuRequest(ctrs, "main"); cpu != "17m" {
					t.Errorf("patched CPU request = %q, want 17m", cpu)
				}
			},
		},
		{
			name:     "in-place mode resizes the pod and leaves the template untouched",
			scenario: scenario{cluster: standardCluster(), mode: k8sclient.ResizeModeInPlace, config: baseConfig},
			assert:   assertResizedNoTemplate,
		},
		{
			name:     "dry-run performs no mutating calls",
			scenario: scenario{cluster: standardCluster(), mode: k8sclient.ResizeModeInPlace, config: baseConfig, dryRun: true},
			assert: func(t *testing.T, rec *requestRecorder) {
				if rec.resizePatched() {
					t.Error("dry-run issued a /resize")
				}
				if rec.templatePatched() {
					t.Error("dry-run patched the template")
				}
				if rec.deleteCount() != 0 || rec.evictionCount() != 0 {
					t.Error("dry-run disrupted pods")
				}
			},
		},
		{
			name: "foreign pod is skipped, owned pod is resized",
			scenario: scenario{
				cluster: fakeCluster{
					cores:  standardCores,
					target: deployment("test-dep", ctr("main", "10m")),
					pods: []apiv1.Pod{
						ownedPod("test-dep-abc-def", "test-dep-abc", ctr("main", "10m")),
						ownedPod("foreign-pod", "other-rs", ctr("main", "10m")),
					},
					hasResize: true,
				},
				mode:   k8sclient.ResizeModeInPlace,
				config: baseConfig,
			},
			assert: func(t *testing.T, rec *requestRecorder) {
				if got := rec.resizePatchCount(); got != 1 {
					t.Fatalf("resize patch count = %d, want 1 (only the owned pod)", got)
				}
				if names := rec.resizedPodNames(); len(names) != 1 || names[0] != "test-dep-abc-def" {
					t.Errorf("resized pods = %v, want [test-dep-abc-def]", names)
				}
			},
		},
		{
			name: "InPlaceOrRecreate degrades to Recreate when pods/resize is absent",
			scenario: scenario{
				cluster: fakeCluster{
					cores:     standardCores,
					target:    deployment("test-dep", ctr("main", "10m")),
					pods:      []apiv1.Pod{ownedPod("test-dep-abc-def", "test-dep-abc", ctr("main", "10m"))},
					hasResize: false,
				},
				mode:   k8sclient.ResizeModeInPlaceOrRecreate,
				config: baseConfig,
			},
			assert: func(t *testing.T, rec *requestRecorder) {
				if !rec.templatePatched() {
					t.Fatal("expected a template patch after degrading to Recreate")
				}
				if rec.resizePatched() {
					t.Error("unexpected /resize after degradation")
				}
			},
		},
		{
			name:     "in-place resizes a DaemonSet pod",
			scenario: scenario{cluster: kindCluster(dsTarget, "test-ds-pod"), mode: k8sclient.ResizeModeInPlace, config: baseConfig},
			assert:   assertResizedNoTemplate,
		},
		{
			name:     "in-place resizes a StatefulSet pod",
			scenario: scenario{cluster: kindCluster(ssTarget, "test-ss-pod"), mode: k8sclient.ResizeModeInPlace, config: baseConfig},
			assert:   assertResizedNoTemplate,
		},
		{
			name:     "in-place resizes a ReplicaSet pod",
			scenario: scenario{cluster: kindCluster(rsTarget, "test-rs-pod"), mode: k8sclient.ResizeModeInPlace, config: baseConfig},
			assert:   assertResizedNoTemplate,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := runPoll(t, tc.scenario)
			if err != nil {
				t.Fatalf("pollAPIServer: %v", err)
			}
			tc.assert(t, rec)
		})
	}
}

// TestPollAPIServer_Fallback covers the InPlaceOrRecreate fallback end-to-end.
// Each case runs two cycles: the first tracks a stuck pod (age below grace, no
// action), then the resizer clock is advanced past the grace period so the
// second cycle takes the fallback path.
func TestPollAPIServer_Fallback(t *testing.T) {
	const baseConfig = `{"main":{"requests":{"cpu":{"base":"10m","step":"1m","coresPerStep":1}}}}`
	standardCores := []int{2, 2, 2, 1}
	const grace = 5 * time.Minute

	// A stuck pod: spec already at the desired 17m (cpvpa resized it on an
	// earlier cycle) but the kubelet reports the resize as Infeasible, so no
	// /resize is issued and it accumulates toward the fallback.
	stuck := apiv1.PodCondition{Type: apiv1.PodResizePending, Status: apiv1.ConditionTrue, Reason: apiv1.PodReasonInfeasible}

	dsTarget := daemonset("test-ds", appsv1.OnDeleteDaemonSetStrategyType, ctr("main", "10m"))
	rsTarget := replicaset("test-rs", ctr("main", "10m"))

	cases := []struct {
		name     string
		scenario scenario
		assert   func(t *testing.T, rec *requestRecorder)
	}{
		{
			name: "self-healing target: stuck pod triggers a template rollout, no disruption",
			scenario: scenario{
				cluster: fakeCluster{
					cores:     standardCores,
					target:    deployment("test-dep", ctr("main", "10m")),
					pods:      []apiv1.Pod{ownedPod("test-dep-abc-def", "test-dep-abc", ctr("main", "17m"), stuck)},
					hasResize: true,
				},
				mode:     k8sclient.ResizeModeInPlaceOrRecreate,
				fallback: k8sclient.ResizeFallbackConfig{GracePeriod: grace, MaxPodsPerCycle: 1},
				config:   baseConfig,
				cycles:   2,
				advance:  grace + time.Minute,
			},
			assert: func(t *testing.T, rec *requestRecorder) {
				if !rec.templatePatched() {
					t.Fatal("expected a template rollout for the stuck self-healing pod")
				}
				if rec.evictionCount() != 0 || rec.deleteCount() != 0 {
					t.Error("self-healing target must not be disrupted directly")
				}
				if rec.resizePatched() {
					t.Error("pod already at desired spec; no /resize expected")
				}
			},
		},
		{
			name: "non-self-healing target (OnDelete DaemonSet): stuck pod is evicted",
			scenario: scenario{
				cluster: fakeCluster{
					cores:     standardCores,
					target:    dsTarget,
					pods:      []apiv1.Pod{directlyOwnedPod(t, "test-ds-pod", dsTarget, ctr("main", "17m"), stuck)},
					hasResize: true,
				},
				mode:     k8sclient.ResizeModeInPlaceOrRecreate,
				fallback: k8sclient.ResizeFallbackConfig{GracePeriod: grace, MaxPodsPerCycle: 1, DisruptionMethod: k8sclient.FallbackDisruptionEviction},
				config:   baseConfig,
				cycles:   2,
				advance:  grace + time.Minute,
			},
			assert: func(t *testing.T, rec *requestRecorder) {
				if rec.evictionCount() != 1 {
					t.Errorf("eviction count = %d, want 1", rec.evictionCount())
				}
				if rec.deleteCount() != 0 {
					t.Errorf("delete count = %d, want 0 (eviction configured)", rec.deleteCount())
				}
				if !rec.templatePatched() {
					t.Error("template should be patched so the recreated pod gets the new size")
				}
			},
		},
		{
			name: "non-self-healing target (ReplicaSet): stuck pod is deleted",
			scenario: scenario{
				cluster: fakeCluster{
					cores:     standardCores,
					target:    rsTarget,
					pods:      []apiv1.Pod{directlyOwnedPod(t, "test-rs-pod", rsTarget, ctr("main", "17m"), stuck)},
					hasResize: true,
				},
				mode:     k8sclient.ResizeModeInPlaceOrRecreate,
				fallback: k8sclient.ResizeFallbackConfig{GracePeriod: grace, MaxPodsPerCycle: 1, DisruptionMethod: k8sclient.FallbackDisruptionDelete},
				config:   baseConfig,
				cycles:   2,
				advance:  grace + time.Minute,
			},
			assert: func(t *testing.T, rec *requestRecorder) {
				if rec.deleteCount() != 1 {
					t.Errorf("delete count = %d, want 1", rec.deleteCount())
				}
				if rec.evictionCount() != 0 {
					t.Errorf("eviction count = %d, want 0 (delete configured)", rec.evictionCount())
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, err := runPoll(t, tc.scenario)
			if err != nil {
				t.Fatalf("pollAPIServer: %v", err)
			}
			tc.assert(t, rec)
		})
	}
}
