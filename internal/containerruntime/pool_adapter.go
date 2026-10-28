package containerruntime

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// stopTimeout is the default time to wait for an orderly shutdown, with a
// termination signal, before the removal of the instance is forced.
const stopTimeout = 10 * time.Second

// containerSocketDir is the path inside the container where the invocation
// socket is mounted. It must match the default of the watchdog of the
// function.
const containerSocketDir = "/run/faas-continuum-socket"

// socketFileName is the name of the socket file inside the mounted
// directory.
const socketFileName = "handle.sock"

// DefaultHostSocketRoot is the default directory on the host where every
// instance gets its own subdirectory for the invocation socket.
const DefaultHostSocketRoot = "/run/faas-continuum/sockets"

// PoolAdapter implements the provisioner interface of the pool over this
// runtime, without this package having to import the pool: the interface
// is satisfied structurally. It is how the pool of minimum instances
// connects to the real integration with the container daemon.
//
// A single adapter instance serves every function of a node: the image is
// resolved on every provisioning call, instead of being fixed at the
// construction, because different functions use different images.
//
// Every provisioned instance gets a container with no network and a Unix
// socket exposed through a mount, which is how the agent talks to the
// process of the function without a network plugin or any port.
type adapterRuntime interface {
	ctx(parent context.Context) context.Context
	PullImage(parent context.Context, ref string) (containerd.Image, error)
	Create(parent context.Context, image containerd.Image, opts CreateOptions) (containerd.Container, error)
	Start(parent context.Context, container containerd.Container) (*Handle, error)
	Stop(parent context.Context, h *Handle, timeout time.Duration) error
	Remove(parent context.Context, container containerd.Container) error
	StopAndRemove(parent context.Context, container containerd.Container) error
	List(parent context.Context) ([]containerd.Container, error)
	LoadContainer(parent context.Context, id string) (containerd.Container, error)
	LoadRunningHandle(parent context.Context, container containerd.Container) (*Handle, error)
}

type PoolAdapter struct {
	runtime        adapterRuntime
	hostSocketRoot string
	// GPUForImage resolves whether the image requests a graphics device.
	// A nil function means that no image requests one, so every
	// container runs without a device. It is configured by the wiring of
	// the agent over the registry. The resolution is per image and not
	// per function, because only the image reference reaches the
	// provisioning, so two functions with the same image and different
	// settings inherit the first one registered.
	GPUForImage func(imageRef string) bool

	mu      sync.Mutex
	handles map[string]*Handle // instanceID -> handle em execução
}

// socketDir returns the path of the socket directory on the host for a
// given instance. The lifecycle of the directory is bound to the creation
// attempt: it is created during the provisioning and removed on the
// termination or on the removal of an existing container, so no orphan
// directory survives the lifecycle of the instance.
func (a *PoolAdapter) socketDir(instanceID string) string {
	return filepath.Join(a.hostSocketRoot, instanceID)
}

// clearSocketDir removes the socket directory of the instance on a
// best-effort basis. The errors are ignored, because the directory is
// removed at the end of the lifecycle of the instance and must never
// leave traces behind.
func (a *PoolAdapter) clearSocketDir(instanceID string) {
	_ = os.RemoveAll(a.socketDir(instanceID))
}

// The socket directory has a lifecycle bound to the creation attempt:
//
//   - the directory is created during the provisioning, when the image is
//     pulled successfully;
//   - it is removed when the provisioning fails at the creation or at the
//     start, with a best-effort cleanup in both branches;
//   - it is removed on the termination or on the removal of an existing
//     container, even when the stop or the removal fails, because the
//     directory has no value without the instance;
//   - the adoption of an existing container creates no directory, and the
//     existing one is kept until the termination.
//
// The invariant is that the life of the directory matches the creation
// attempt, and that the entry in the map of handles lasts from a
// successful provisioning or adoption until the termination.

// NewPoolAdapterWithSocketRoot returns a pool adapter for the given
// runtime, allowing the base directory of the sockets to be chosen, which
// is useful in a test environment where the usual runtime directory may
// not be writable.
func NewPoolAdapterWithSocketRoot(runtime adapterRuntime, hostSocketRoot string) *PoolAdapter {
	return &PoolAdapter{
		runtime:        runtime,
		hostSocketRoot: hostSocketRoot,
		handles:        make(map[string]*Handle),
	}
}

// SocketWatchDir returns the directory on the host that the readiness
// watcher must observe for an instance, which is exactly the socket
// directory of that instance.
//
// The instance identifier is sanitized with a path cleaning and a prefix
// check under the host socket root, so a traversal attempt or an empty
// directory returns an error before any watcher is created. The consumers
// treat the error as a closed failure and never fall back to the root or
// to a temporary directory.
func (a *PoolAdapter) SocketWatchDir(instanceID string) (string, error) {
	if instanceID == "" {
		return "", fmt.Errorf("containerruntime: SocketWatchDir com instanceID vazio (fail-closed)")
	}
	root := filepath.Clean(a.hostSocketRoot)
	dir := filepath.Clean(filepath.Join(root, instanceID))
	if dir == root || !strings.HasPrefix(dir, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("containerruntime: SocketWatchDir com instanceID %q fora de %q (fail-closed)", instanceID, root)
	}
	return dir, nil
}

// SocketPath returns the path on the host of the Unix socket of the
// invocation of an instance. It is how the agent talks to the process of
// the function, with no network involved.
func (a *PoolAdapter) SocketPath(instanceID string) string {
	return filepath.Join(a.socketDir(instanceID), socketFileName)
}

// prepareSocketDir creates the socket directory of an instance with fully
// open permissions.
//
// This is necessary because the framework neither controls nor knows the
// identifier under which the process of the function runs inside the
// container: the image of the function decides that, for instance with a
// user directive in its Dockerfile. The directory may be created by the
// agent process, which frequently runs as root or with a different
// identifier than the image, and then mounted inside the container.
//
// Without open permissions the watchdog inside the container cannot
// create the socket file, because binding a Unix socket requires write
// permission on the directory, exactly like creating any other file. The
// watchdog always removes and recreates that file from scratch on every
// start, so that it never inherits a stale socket of a previous run,
// which means the problem cannot be worked around by creating the file
// once with a stricter permission.
//
// This is the same trade-off, for the same reason, as the socket of the
// container daemon itself: when the consumer of a Unix socket may run
// under any unknown identifier, the usual practice is to open the
// permission of that specific resource instead of trying to match the
// identifiers, which would require a convention shared between the
// framework and every author of a function image, for a marginal gain in
// isolation on a single-tenant edge device.
//
// The explicit change of mode after the creation of the directory is
// necessary because the mask of permissions of the process, frequently
// 022 on Linux systems and even when running as root, would reduce the
// requested mode to something stricter at creation time, while the mask
// only affects the permission requested at creation and never an explicit
// change of mode afterwards.
//
// The scope of the trade-off is that the directory belongs to a single
// instance, one subdirectory per instance under the host socket root, and
// is not shared between functions. Another local process with access to
// that specific path could talk directly to the process of the function
// without going through the agent, but that is not a new exposure: an
// invocation never requires a management credential by design, since only
// the management operations do. Hardening this properly, by discovering
// and matching the real identifier of the image, is a possible future
// work and is not solved here.
func prepareSocketDir(path string) error {
	if err := os.MkdirAll(path, 0o777); err != nil {
		return err
	}
	return os.Chmod(path, 0o777)
}

// AdoptHandle records, in the internal accounting of the adapter, a
// handle loaded by the restart reconciliation. Without it a later
// termination of that instance would fail, because the map of handles of
// the adapter starts empty on every new process of the agent. The
// accounting is only in memory: the containers themselves survive in the
// container daemon, independently of the process of the agent.
func (a *PoolAdapter) AdoptHandle(instanceID string, handle *Handle) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.handles[instanceID] = handle
}

// provisionCreateOptions builds the options of creation of the container
// of an instance: only the identifier, the mount of the Unix socket and,
// when it is requested, the graphics device.
//
// It deliberately sets no arbitrary environment variable: the process of
// the function inherits only the default environment of the image, so
// the secrets of the agent never leak into the function. The single
// exception is that, when the device is requested, the creation adds only
// the device selectors, which select a device and are not secrets.
//
// It is extracted into a pure helper so that a test can pin it without a
// container daemon.
func provisionCreateOptions(instanceID, hostSocketDir string, gpu bool) CreateOptions {
	return CreateOptions{
		ID:  instanceID,
		GPU: gpu,
		Mounts: []specs.Mount{
			{
				Type:        "bind",
				Source:      hostSocketDir,
				Destination: containerSocketDir,
				Options:     []string{"rbind", "rw"},
			},
		},
	}
}

// Provision pulls the image, when it is not available yet, prepares the
// directory of the invocation socket and creates and starts a container
// instance with the given identifier.
func (a *PoolAdapter) Provision(ctx context.Context, instanceID, imageRef string) error {
	image, err := a.runtime.PullImage(ctx, imageRef)
	if err != nil {
		return err
	}

	hostSocketDir := a.socketDir(instanceID)
	if err := prepareSocketDir(hostSocketDir); err != nil {
		return fmt.Errorf("containerruntime: falha ao preparar diretório do socket de %q: %w", instanceID, err)
	}

	gpu := false
	if a.GPUForImage != nil {
		gpu = a.GPUForImage(imageRef)
	}
	container, err := a.runtime.Create(ctx, image, provisionCreateOptions(instanceID, hostSocketDir, gpu))
	if err != nil {
		// This mirrors the branch of the start: without this cleanup the
		// socket directory created above would leak on every failed
		// creation, since the handle is never registered and no later
		// termination would remove it.
		a.clearSocketDir(instanceID)
		return err
	}

	handle, err := a.runtime.Start(ctx, container)
	if err != nil {
		// Without this cleanup the newly created container would remain
		// an orphan in the daemon until the next restart reconciliation,
		// wasting a snapshot and memory on a constrained device. The
		// cleanup is best-effort: when it fails, the startup
		// reconciliation removes the container afterwards.
		if cleanupErr := a.runtime.StopAndRemove(ctx, container); cleanupErr != nil {
			log.Printf("containerruntime: falha ao limpar contêiner %q após erro de start: %v", instanceID, cleanupErr)
		}
		a.clearSocketDir(instanceID)
		return err
	}

	a.mu.Lock()
	a.handles[instanceID] = handle
	a.mu.Unlock()
	return nil
}

// Terminate stops and removes the instance that was provisioned before,
// and cleans the socket directory associated with it.
//
// The handle only leaves the accounting when both the stop and the
// removal succeed: removing it earlier would make an intermediate failure
// leave the container unreachable for later attempts, since the client
// would receive an error instead of an instance-not-found answer while
// the container was still running. The socket directory is removed even
// on the error paths, because it has no value without the instance.
func (a *PoolAdapter) Terminate(ctx context.Context, instanceID string) error {
	a.mu.Lock()
	handle, ok := a.handles[instanceID]
	a.mu.Unlock()

	if !ok {
		return fmt.Errorf("containerruntime: instância %q não encontrada neste adaptador", instanceID)
	}

	err := a.runtime.Stop(ctx, handle, stopTimeout)
	if err == nil {
		err = a.runtime.Remove(ctx, handle.Container)
	}
	a.clearSocketDir(instanceID) // socket dir removed even on error paths
	if err != nil {
		a.mu.Lock()
		delete(a.handles, instanceID)
		a.mu.Unlock()
		return err
	}

	a.mu.Lock()
	delete(a.handles, instanceID)
	a.mu.Unlock()
	return nil
}

// Operations based on the instance identifier, used by the restart
// reconciliation.
//
// They are deliberately expressed in terms of the identifier and not of
// the types of the container daemon, so that the reconciliation stays
// decoupled from the real daemon and can be tested with a fake.

// ListInstanceIDs returns the identifiers of all the containers present
// in the namespace of the framework, including those that survived a
// previous restart of the agent.
func (a *PoolAdapter) ListInstanceIDs(ctx context.Context) ([]string, error) {
	containers, err := a.runtime.List(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(containers))
	for i, c := range containers {
		ids[i] = c.ID()
	}
	return ids, nil
}

// AdoptExisting loads the task of an existing container, confirms that it
// is running and, when it is, records the resulting handle in the
// accounting of the adapter.
//
// It returns the not-running error when the container exists but has no
// running task, and it is up to the caller to decide to remove it instead
// of adopting it. Any other error is also returned to the caller.
func (a *PoolAdapter) AdoptExisting(ctx context.Context, instanceID string) error {
	container, err := a.runtime.LoadContainer(ctx, instanceID)
	if err != nil {
		return err
	}

	handle, err := a.runtime.LoadRunningHandle(ctx, container)
	if err != nil {
		return err
	}

	// A nil error and a non-nil handle mean that the task is running and
	// the container is adopted.
	a.AdoptHandle(instanceID, handle)
	return nil
}

// StopAndRemoveExisting stops and removes an orphan container, one with
// no matching function in the persisted registry, or an unhealthy one,
// whose task is not running, found by the restart reconciliation. It also
// cleans the socket directory associated with it.
func (a *PoolAdapter) StopAndRemoveExisting(ctx context.Context, instanceID string) error {
	container, err := a.runtime.LoadContainer(ctx, instanceID)
	if err != nil {
		return err
	}
	if err := a.runtime.StopAndRemove(ctx, container); err != nil {
		return err
	}
	a.clearSocketDir(instanceID)
	return nil
}
