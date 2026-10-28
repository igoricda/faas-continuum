// Package containerruntime encapsulates the integration of the framework
// with containerd through the official Go SDK. The low-level runtime
// acts only as a shim underneath containerd, and this package never talks
// to the daemon of the container runtime directly.
//
// It covers the basic lifecycle of the containers: create, start, stop
// and collect metrics. The maintenance of the pool of pre-allocated
// instances is built on top of these primitives, which keeps this layer
// of integration lean and independently testable.
package containerruntime

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	"github.com/igoricda/faas-continuum/internal/imageref"
)

// DefaultSocket is the default address of the socket of the container
// daemon on most Linux distributions. It can be overridden through
// Config.SocketPath for a customized installation.
const DefaultSocket = "/run/containerd/containerd.sock"

// DefaultNamespace isolates the resources of the framework inside the
// container daemon from the other workloads that may run on the same
// device.
const DefaultNamespace = "faas-continuum"

// Config parameterizes the connection with the local container daemon of
// the node.
type Config struct {
	SocketPath string
	Namespace  string
}

// Runtime is the single point of access of the node agent to the
// container daemon. One instance is created per agent process and reused
// for every lifecycle operation of the containers.
type Runtime struct {
	client    *containerd.Client
	namespace string
}

// New connects to the local container daemon. It should be called once,
// during the startup of the node agent.
func New(cfg Config) (*Runtime, error) {
	socket := cfg.SocketPath
	if socket == "" {
		socket = DefaultSocket
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = DefaultNamespace
	}

	client, err := containerd.New(socket)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao conectar ao containerd em %q: %w", socket, err)
	}

	return &Runtime{client: client, namespace: ns}, nil
}

func (r *Runtime) Close() error {
	return r.client.Close()
}

// ctx returns a context bound to the namespace of the framework, which
// the container daemon requires on every call of the API.
func (r *Runtime) ctx(parent context.Context) context.Context {
	return namespaces.WithNamespace(parent, r.namespace)
}

// PullImage guarantees that the referenced image is available locally
// before the container is created.
//
// The reference is normalized before it is resolved, with the same rules
// the command line client applies behind the scenes: a name without a
// registry becomes an official image of the default registry with the
// latest tag. Without this normalization the raw client of the container
// daemon reads the first segment before the slash as the host of a
// registry, and tries to resolve it through the name service instead of
// assuming the default registry.
func (r *Runtime) PullImage(parent context.Context, ref string) (containerd.Image, error) {
	normalized, err := imageref.NormalizeImageRef(ref)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: referência de imagem %q inválida: %w", ref, err)
	}

	ctx := r.ctx(parent)

	// An image already present in the local content store is reused
	// without contacting the registry: during a scale-up under load
	// every provisioning would otherwise pay a network round trip to
	// resolve a manifest that is already on disk, which is pure
	// latency on a node with an intermittent link.
	if image, err := r.client.GetImage(ctx, normalized); err == nil {
		return image, nil
	} else if !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("containerruntime: falha ao verificar imagem %q no armazenamento local: %w", normalized, err)
	}

	image, err := r.client.Pull(ctx, normalized, containerd.WithPullUnpack)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao baixar imagem %q (normalizada de %q): %w", normalized, ref, err)
	}
	return image, nil
}

// CreateOptions describes the minimum parameters needed to create a
// container of a function. The resource fields, such as the limits of
// CPU and memory, extend it when the pre-allocation pool needs them.
type CreateOptions struct {
	// ID is the unique identifier of the container inside the namespace
	// of the framework, typically derived from the resource URL of the
	// function.
	ID string
	// ImageRef is the reference of the image, already resolved by PullImage.
	ImageRef string
	// Args overrides the entry point and the command of the image.
	Args []string
	// Mounts builds the paths of the host inside the container. It is
	// used to expose the Unix socket of the invocation without any
	// network between the agent and the container.
	Mounts []specs.Mount
	// Env defines the additional environment variables of the container.
	Env []string
	// GPU requests the graphics device in the container through the
	// prestart hook below. It is resolved by the pool adapter from the
	// settings of the function and is false by default, which means a
	// container with no device and no change to the runtime.
	GPU bool
}

// Handle represents a container managed by the framework, with its task
// already running when one exists.
type Handle struct {
	Container containerd.Container
	Task      containerd.Task
}

// nvidiaHookPath is the prestart hook of the graphics driver library: the
// low-level runtime executes it before the initialization of the
// container, and it injects the libraries of the driver and the device
// nodes by reading the visible devices and the capabilities from the
// environment of the container.
//
// It requires the container toolkit of the driver on the host. Without it
// the creation of a container that requests the device fails explicitly,
// because the hook does not exist, and never silently.
const nvidiaHookPath = "/usr/bin/nvidia-container-runtime-hook"

// withNvidiaHook attaches the prestart hook of the graphics driver to the
// specification of the container. It is pure, since it only mutates the
// specification it receives, so a test can pin it without a daemon.
func withNvidiaHook() oci.SpecOpts {
	return func(_ context.Context, _ oci.Client, _ *containers.Container, s *oci.Spec) error {
		if s.Hooks == nil {
			s.Hooks = &specs.Hooks{}
		}
		s.Hooks.Prestart = append(s.Hooks.Prestart, specs.Hook{
			Path: nvidiaHookPath,
			Args: []string{"nvidia-container-runtime-hook", "prestart"},
			Env:  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
		})
		return nil
	}
}

// Create creates a new container from an image that was already pulled,
// without starting it. Separating the creation from the start lets the
// pool keep containers ready without consuming CPU before the first
// invocation.
//
// The container comes up with its own network namespace, isolated and
// empty, with neither a network plugin nor the network of the host: the
// only way to reach the process of the function is the Unix socket
// exposed through the mounts, so a container of a function neither needs
// nor should see any network.
func (r *Runtime) Create(parent context.Context, image containerd.Image, opts CreateOptions) (containerd.Container, error) {
	if opts.ID == "" {
		return nil, errors.New("containerruntime: CreateOptions.ID é obrigatório")
	}
	ctx := r.ctx(parent)

	specOpts := []oci.SpecOpts{
		oci.WithImageConfig(image),
		oci.WithLinuxNamespace(specs.LinuxNamespace{Type: specs.NetworkNamespace}),
	}
	if len(opts.Args) > 0 {
		specOpts = append(specOpts, oci.WithProcessArgs(opts.Args...))
	}
	if len(opts.Mounts) > 0 {
		specOpts = append(specOpts, oci.WithMounts(opts.Mounts))
	}
	if len(opts.Env) > 0 {
		specOpts = append(specOpts, oci.WithEnv(opts.Env))
	}
	if opts.GPU {
		// The selectors read by the prestart hook, which are device
		// selections and not secrets.
		specOpts = append(specOpts,
			withNvidiaHook(),
			oci.WithEnv([]string{
				"NVIDIA_VISIBLE_DEVICES=all",
				"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
			}),
		)
	}

	container, err := r.client.NewContainer(
		ctx,
		opts.ID,
		containerd.WithNewSnapshot(opts.ID+"-snapshot", image),
		containerd.WithNewSpec(specOpts...),
	)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao criar contêiner %q: %w", opts.ID, err)
	}
	return container, nil
}

// Start begins the execution of a container that was previously created.
// It returns a handle whose task is already running and ready to receive
// the requests of the function.
func (r *Runtime) Start(parent context.Context, container containerd.Container) (*Handle, error) {
	ctx := r.ctx(parent)

	task, err := container.NewTask(ctx, cio.NewCreator(cio.WithStdio))
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao criar task para %q: %w", container.ID(), err)
	}

	if err := task.Start(ctx); err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao iniciar task para %q: %w", container.ID(), err)
	}

	return &Handle{Container: container, Task: task}, nil
}

// Stop ends the execution of the container in an orderly way, with a
// termination signal, waiting until the timeout before considering the
// operation finished. It does not remove the container from the daemon,
// which is the job of Remove.
func (r *Runtime) Stop(parent context.Context, h *Handle, timeout time.Duration) error {
	ctx := r.ctx(parent)

	exitCh, err := h.Task.Wait(ctx)
	if err != nil {
		return fmt.Errorf("containerruntime: falha ao aguardar task %q: %w", h.Container.ID(), err)
	}

	if err := h.Task.Kill(ctx, syscall.SIGTERM); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("containerruntime: falha ao enviar SIGTERM para %q: %w", h.Container.ID(), err)
	}

	select {
	case <-exitCh:
	case <-time.After(timeout):
		if err := h.Task.Kill(ctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("containerruntime: falha ao enviar SIGKILL para %q: %w", h.Container.ID(), err)
		}
		<-exitCh
	}

	if _, err := h.Task.Delete(ctx); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("containerruntime: falha ao remover task %q: %w", h.Container.ID(), err)
	}
	return nil
}

// Remove deletes the container and its snapshot from the daemon. It should
// be called after Stop.
func (r *Runtime) Remove(parent context.Context, container containerd.Container) error {
	ctx := r.ctx(parent)
	if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("containerruntime: falha ao remover contêiner %q: %w", container.ID(), err)
	}
	return nil
}

// List enumerates the containers currently managed by the framework in the
// configured namespace, which is used to rebuild the state of the pool of
// instances after the agent restarts.
func (r *Runtime) List(parent context.Context) ([]containerd.Container, error) {
	ctx := r.ctx(parent)
	containers, err := r.client.Containers(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao listar contêineres: %w", err)
	}
	return containers, nil
}

// LoadContainer loads an existing container by its identifier, which the
// restart reconciliation uses to resume the containers created by a
// previous process of the agent.
func (r *Runtime) LoadContainer(parent context.Context, id string) (containerd.Container, error) {
	ctx := r.ctx(parent)
	container, err := r.client.LoadContainer(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao carregar contêiner %q: %w", id, err)
	}
	return container, nil
}

// ErrNotRunning is returned by LoadRunningHandle when the container exists
// but its task is not running, or has no task at all.
var ErrNotRunning = errors.New("containerruntime: not running")

// LoadRunningHandle loads the task of a container that existed before this
// process of the agent, such as a survivor of a restart, and confirms that
// it is really running. A container without a task, either because it
// never started or because the task already died, and a container whose
// task is not running return ErrNotRunning, and it is up to the caller to
// decide to remove them, since they do not count as healthy for the
// adoption in the pool.
func (r *Runtime) LoadRunningHandle(parent context.Context, container containerd.Container) (*Handle, error) {
	ctx := r.ctx(parent)

	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, ErrNotRunning // without a task: not an error, the container is only not running
		}
		return nil, fmt.Errorf("containerruntime: falha ao carregar task de %q: %w", container.ID(), err)
	}

	status, err := task.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerruntime: falha ao consultar status da task de %q: %w", container.ID(), err)
	}
	if status.Status != containerd.Running {
		return nil, ErrNotRunning
	}

	return &Handle{Container: container, Task: task}, nil
}

// StopAndRemove deletes a container whose real state is not entirely
// trustworthy, which the restart reconciliation uses to clean the orphan
// containers, whose function is gone, and the unhealthy ones, whose task
// is not running, left behind by a previous process of the agent.
//
// It kills the process instead of performing the orderly shutdown of
// Stop, because there is no guarantee that the process of the container
// still reacts to a signal in a predictable way: it may already be dead,
// it may be in any state, including a container that never started. Even
// when it dies without a chance of an orderly shutdown, the container is
// no longer serving requests, so there is nothing graceful to preserve.
//
// The daemon requires the task to be deleted before the container,
// whatever its state, because deleting the container directly while the
// task is not running still fails with a precondition error. Killing the
// process and deleting happen in a single call, which is the same
// sequence used by the container runtime implementations: kill the task,
// delete the task, then delete the container.
func (r *Runtime) StopAndRemove(parent context.Context, container containerd.Container) error {
	ctx := r.ctx(parent)

	task, err := container.Task(ctx, nil)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("containerruntime: falha ao carregar task de %q: %w", container.ID(), err)
	}
	if task != nil {
		if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("containerruntime: falha ao apagar task de %q: %w", container.ID(), err)
		}
	}

	return r.Remove(parent, container)
}
