package mcpserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/ruiheng/waypost/internal/version"
	"github.com/ruiheng/waypost/internal/waypost"
)

const (
	serverName                   = "waypost"
	serverVersion                = version.Version
	syncCmdTimeout               = 30 * time.Second
	ensureSessionShowTimeout     = 30 * time.Second
	defaultMCPLeaseTTL           = 30 * time.Second
	defaultLeaseRenewInterval    = 10 * time.Second
	notificationDelivery         = "delivery_available"
	notificationFallbackWake     = "fallback_wake"
	waypostOverviewURI           = "waypost://bound/overview"
	defaultWakePollInterval      = 30 * time.Second
	defaultTargetedWakeTimeout   = 15 * time.Second
	defaultWakeInterChannelGap   = 1 * time.Minute
	defaultMCPHintInitialDelay   = 1 * time.Minute
	defaultMCPHintCooldown       = 2 * time.Minute
	defaultAgentDeckInitialDelay = 3 * time.Minute
	defaultAgentDeckCooldown     = 5 * time.Minute
	defaultNotifyDelay           = 2 * time.Second
	defaultStartupInstruction    = ""
	defaultNotifyMessage         = "NOTICE: There might be new delivery in waypost."
	agentDeckBindRecoveryHint    = "agent-deck address auto-bind did not find your current session; run `agent-deck session current --json` to find your `agent-deck/<session-id>` address, then call `waypost_bind` with that address."
	toolSessionBindRecoveryHint  = "AI tool session auto-bind did not find codex/..., claude/..., gemini/..., opencode/..., or devin/...; expose CODEX_THREAD_ID, CLAUDE_CODE_SESSION_ID, GEMINI_SESSION_ID, OPENCODE_SESSION_ID, or DEVIN_SESSION_ID, wait for agent-deck state sync for the current session, then call `waypost_status` again or call `waypost_bind` manually."
	unsetValue                   = "<unset>"
)

type Runner interface {
	Run(ctx context.Context, args []string, input string) (RunResult, error)
}

type RunResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

type waypostServiceFactory interface {
	Open(context.Context) (any, func() error, error)
}

type waypostSender interface {
	Send(context.Context, waypost.SendParams) (waypost.SendResult, error)
}

type waypostLister interface {
	List(context.Context, waypost.ListParams) ([]waypost.ListedDelivery, error)
}

type waypostGroupMessageLister interface {
	ListGroupMessages(context.Context, waypost.GroupListParams) ([]waypost.GroupListedMessage, error)
}

type waypostGroupMessageWaiter interface {
	WaitGroupMessage(context.Context, waypost.GroupWaitParams) (waypost.GroupListedMessage, error)
}

type waypostGroupMessageReceiver interface {
	ReceiveGroupMessage(context.Context, waypost.GroupReceiveParams) (waypost.GroupReceivedMessage, error)
}

type waypostGroupManager interface {
	CreateGroup(context.Context, string) (waypost.GroupRecord, error)
	AddGroupMember(context.Context, string, string) (waypost.GroupMembershipRecord, error)
	RemoveGroupMember(context.Context, string, string) (waypost.GroupMembershipRecord, error)
	ListGroupMembers(context.Context, string) ([]waypost.GroupMembershipRecord, error)
}

type waypostGroupSubscriberManager interface {
	AddGroupNotificationSubscriber(context.Context, string, string, string) (waypost.GroupNotificationSubscriberRecord, error)
	RemoveGroupNotificationSubscriber(context.Context, string, string) (waypost.GroupNotificationSubscriberRecord, error)
	ListGroupNotificationSubscribers(context.Context, string) ([]waypost.GroupNotificationSubscriberRecord, error)
}

type waypostAddressInspector interface {
	InspectAddress(context.Context, string) (waypost.AddressInspection, error)
}

type waypostClaimableLister interface {
	ListClaimableAddresses(context.Context, []string) ([]waypost.ClaimableAddress, error)
}

type waypostBatchReceiver interface {
	ReceiveBatchWithLeaseTTL(context.Context, waypost.ReceiveBatchParams, time.Duration) (waypost.ReceiveResult, error)
}

type waypostWaiter interface {
	Wait(context.Context, waypost.WaitParams) (waypost.ListedDelivery, error)
}

type waypostDeliveryReader interface {
	ReadDeliveries(context.Context, []string) ([]waypost.ReadDelivery, error)
}

type waypostMessageReader interface {
	ReadMessages(context.Context, []string) ([]waypost.ReadMessage, error)
}

type waypostLatestDeliveryReader interface {
	ReadLatestDeliveries(context.Context, []string, string, int) ([]waypost.ReadDelivery, bool, error)
}

type waypostDeliveryTransitioner interface {
	Ack(context.Context, string, string) (waypost.DeliveryTransitionResult, error)
	Release(context.Context, string, string) (waypost.DeliveryTransitionResult, error)
	Defer(context.Context, string, string, time.Time) (waypost.DeliveryTransitionResult, error)
	Undefer(context.Context, string) (waypost.DeliveryTransitionResult, error)
	Fail(context.Context, string, string, string) (waypost.DeliveryTransitionResult, error)
}

type waypostLeaseRenewer interface {
	Renew(context.Context, string, string, time.Duration) (waypost.LeaseRenewResult, error)
}

type waypostLeaseInspector interface {
	InspectDeliveryLease(context.Context, string) (waypost.DeliveryLeaseState, error)
}

type waypostRemainingCounter interface {
	RemainingByState(context.Context, []string, []string) (map[string]int, error)
}

type runtimeWaypostServiceFactory struct {
	stateDir     string
	openRuntime  func(context.Context, string) (*waypost.Runtime, error)
	closeRuntime func(*waypost.Runtime) error

	mu      sync.Mutex
	service any
	runtime *waypost.Runtime
	closed  bool
}

func (f *runtimeWaypostServiceFactory) Open(ctx context.Context) (any, func() error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, nil, errors.New("waypost runtime is closed")
	}
	if f.service != nil {
		return f.service, func() error { return nil }, nil
	}
	runtime, err := f.openRuntime(ctx, f.stateDir)
	if err != nil {
		return nil, nil, err
	}
	f.runtime = runtime
	f.service = waypost.NewOperations(runtime.Store())
	return f.service, func() error { return nil }, nil
}

func (f *runtimeWaypostServiceFactory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	if f.runtime == nil {
		return nil
	}
	closeRuntime := f.closeRuntime
	if closeRuntime == nil {
		closeRuntime = func(runtime *waypost.Runtime) error {
			return runtime.Close()
		}
	}
	err := closeRuntime(f.runtime)
	f.runtime = nil
	f.service = nil
	return err
}

type Options struct {
	WaypostServiceFactory waypostServiceFactory
	CommandRunner         Runner
	StateDir              string
	Executable            string
	IncludeDebugTool      bool
	Now                   func() time.Time
	MCPLeaseTTL           time.Duration
	LeaseRenewInterval    time.Duration
	DisableLeaseRenewLoop bool
	WakePollInterval      time.Duration
	NotifyDelay           time.Duration
	DisableWakeScheduler  bool
}

type Service struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	waypostServices        waypostServiceFactory
	closeWaypostServices   func() error
	commandRunner          Runner
	sessions               *sessionManager
	notifications          *notificationManager
	activeLeases           *activeLeaseManager
	waypostRecvGuard       *waypostRecvGuard
	state                  *serverState
	now                    func() time.Time
	mcpLeaseTTL            time.Duration
	leaseRenewInterval     time.Duration
	disableLeaseRenewLoop  bool
	wakePollInterval       time.Duration
	targetedWakeTimeout    time.Duration
	notifyDelay            time.Duration
	disableWakeScheduler   bool
	wakeSchedulerState     *wakeSchedulerState
	overviewSubscriptions  *resourceSubscriptionState
	waypostOverviewEmitter func(context.Context) notificationOutcome
	configuredStateDir     string
	executable             string
	includeDebugTool       bool
	leaseRenewLoopOnce     sync.Once
	wakeSchedulerLoopOnce  sync.Once
	backgroundMu           sync.Mutex
	backgroundLoops        sync.WaitGroup
	closeOnce              sync.Once
	closeWaypostOnce       sync.Once
	serverMu               sync.Mutex
	server                 *mcp.Server
}

type runOptions struct {
	input   string
	okCodes []int
	timeout time.Duration
}

type osCommandRunner struct {
	cwd string
}

var legacyServerServices sync.Map

// New returns a legacy MCP server handle. Call CloseServer with the returned
// server when done so service-owned background loops and runtimes are released.
// Prefer NewService when the caller can own the Service lifecycle directly.
func New(opts Options) *mcp.Server {
	service := NewService(opts)
	server := service.Server()
	legacyServerServices.Store(server, service)
	return server
}

// CloseServer closes the Service created by New. It is safe to call more than
// once for the same server.
func CloseServer(server *mcp.Server) {
	if server == nil {
		return
	}
	service, ok := legacyServerServices.LoadAndDelete(server)
	if !ok {
		return
	}
	service.(*Service).Close()
}

func NewService(opts Options) *Service {
	if opts.NotifyDelay == 0 {
		opts.NotifyDelay = defaultNotifyDelay
	}
	return newService(opts)
}

func newService(opts Options) *Service {
	var closeWaypostServices func() error
	if opts.WaypostServiceFactory == nil {
		factory := &runtimeWaypostServiceFactory{
			stateDir:    opts.StateDir,
			openRuntime: waypost.OpenRuntime,
		}
		opts.WaypostServiceFactory = factory
		closeWaypostServices = factory.Close
	}
	if opts.CommandRunner == nil {
		opts.CommandRunner = osCommandRunner{cwd: currentWorkingDir()}
	}
	state := &serverState{}
	sessions := newSessionManager(opts.CommandRunner, state)
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{
		ctx:                   ctx,
		cancel:                cancel,
		waypostServices:       opts.WaypostServiceFactory,
		closeWaypostServices:  closeWaypostServices,
		commandRunner:         opts.CommandRunner,
		sessions:              sessions,
		state:                 state,
		now:                   opts.Now,
		mcpLeaseTTL:           opts.MCPLeaseTTL,
		leaseRenewInterval:    opts.LeaseRenewInterval,
		disableLeaseRenewLoop: opts.DisableLeaseRenewLoop,
		wakePollInterval:      opts.WakePollInterval,
		targetedWakeTimeout:   defaultTargetedWakeTimeout,
		notifyDelay:           opts.NotifyDelay,
		disableWakeScheduler:  opts.DisableWakeScheduler,
		configuredStateDir:    opts.StateDir,
		executable:            opts.Executable,
		includeDebugTool:      opts.IncludeDebugTool,
	}
	if service.now == nil {
		service.now = func() time.Time {
			return time.Now().UTC()
		}
	}
	if service.mcpLeaseTTL <= 0 {
		service.mcpLeaseTTL = defaultMCPLeaseTTL
	}
	if service.leaseRenewInterval <= 0 {
		service.leaseRenewInterval = defaultLeaseRenewInterval
	}
	if service.wakePollInterval <= 0 {
		service.wakePollInterval = defaultWakePollInterval
	}
	if service.notifyDelay < 0 {
		service.notifyDelay = 0
	}
	service.notifications = newNotificationManager(service.commandRunner, service.sessions)
	service.activeLeases = newActiveLeaseManager()
	service.waypostRecvGuard = newWaypostRecvGuard()
	service.wakeSchedulerState = newWakeSchedulerState()
	service.overviewSubscriptions = newResourceSubscriptionState()
	service.waypostOverviewEmitter = service.emitWaypostOverviewUpdated
	return service
}

func (s *Service) executableAndStateDir() (string, string, error) {
	executable := s.executable
	if executable == "" {
		var err error
		executable, err = os.Executable()
		if err != nil {
			return "", "", fmt.Errorf("resolve waypost executable: %w", err)
		}
	}
	executable, err := filepath.Abs(executable)
	if err != nil {
		return "", "", fmt.Errorf("resolve absolute waypost executable: %w", err)
	}
	stateDir, err := waypost.ResolveStateDir(s.configuredStateDir)
	if err != nil {
		return "", "", fmt.Errorf("resolve waypost state directory: %w", err)
	}
	stateDir, err = filepath.Abs(stateDir)
	if err != nil {
		return "", "", fmt.Errorf("resolve absolute waypost state directory: %w", err)
	}
	return executable, stateDir, nil
}

// Close stops service-owned background loops and closes the service-owned
// waypost runtime. It does not wait for in-flight MCP handlers; callers that
// need handler quiescence should close and drain their MCP sessions separately.
func (s *Service) Close() {
	s.closeOnce.Do(func() {
		s.backgroundMu.Lock()
		s.cancel()
		s.backgroundMu.Unlock()
	})
	s.backgroundLoops.Wait()
	s.closeWaypostOnce.Do(func() {
		if s.closeWaypostServices != nil {
			_ = s.closeWaypostServices()
		}
	})
}

func (s *Service) startBackgroundLoop(once *sync.Once, run func()) {
	once.Do(func() {
		s.backgroundMu.Lock()
		defer s.backgroundMu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		s.backgroundLoops.Add(1)
		go func() {
			defer s.backgroundLoops.Done()
			run()
		}()
	})
}

func (s *Service) Server() *mcp.Server {
	s.serverMu.Lock()
	defer s.serverMu.Unlock()
	if s.server != nil {
		return s.server
	}

	server := mcp.NewServer(&mcp.Implementation{Name: serverName, Version: serverVersion}, &mcp.ServerOptions{
		Instructions:       serverInstructions(s.includeDebugTool),
		SubscribeHandler:   s.subscribeResource,
		UnsubscribeHandler: s.unsubscribeResource,
	})

	s.registerWaypostTools(server)
	s.registerSessionTools(server)
	s.registerWaypostOverviewResource(server)
	s.server = server
	return server
}

func serverInstructions(includeDebugTool bool) string {
	statusGate := "Once after this MCP server starts, call waypost_status before the first waypost_* tool."
	if includeDebugTool {
		statusGate = "Once after this MCP server starts, call waypost_status before the first waypost_* tool other than waypost_debug."
	}
	return statusGate + " It auto-binds detectable session addresses and reports warnings.\n" +
		"This server automatically renews leases for personal deliveries claimed by waypost_recv until it stops or restarts.\n" +
		"Waypost is for durable asynchronous work, not real-time communication. MCP covers common operations. For complete Waypost guidance:\n" +
		"  <executable> doc\n" +
		"  <executable> doc --list\n" +
		"  <executable> doc <topic>...\n" +
		"Use the reported executable and resolved_state_dir for stateful CLI commands; never guess either."
}

func currentWorkingDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func (r osCommandRunner) Run(ctx context.Context, args []string, input string) (RunResult, error) {
	if len(args) == 0 {
		return RunResult{}, errors.New("missing command")
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	if r.cwd != "" {
		cmd.Dir = r.cwd
	}
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		return RunResult{ExitCode: 0, Stdout: stdout.String(), Stderr: stderr.String()}, nil
	}
	if ctx.Err() != nil {
		return RunResult{}, ctx.Err()
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return RunResult{ExitCode: exitErr.ExitCode(), Stdout: stdout.String(), Stderr: stderr.String()}, nil
	}
	return RunResult{}, err
}

func withWaypostService[T any, S any](ctx context.Context, factory waypostServiceFactory, fn func(S) (T, error)) (T, error) {
	var zero T
	rawService, closeFunc, err := factory.Open(ctx)
	if err != nil {
		return zero, err
	}
	defer closeFunc()
	service, ok := rawService.(S)
	if !ok {
		return zero, fmt.Errorf("waypost service %T does not satisfy %T", rawService, service)
	}
	return fn(service)
}

func runCommand(ctx context.Context, runner Runner, args []string, opts runOptions) (RunResult, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if opts.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}

	result, err := runner.Run(runCtx, args, opts.input)
	if err != nil {
		detail := err.Error()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return RunResult{}, fmt.Errorf("command failed: %s :: timed out after %dms: %w", strings.Join(args, " "), opts.timeout.Milliseconds(), context.DeadlineExceeded)
		}
		if errors.Is(err, context.Canceled) || errors.Is(runCtx.Err(), context.Canceled) {
			return RunResult{}, fmt.Errorf("command failed: %s :: canceled: %w", strings.Join(args, " "), context.Canceled)
		}
		return RunResult{}, fmt.Errorf("command failed: %s :: %s", strings.Join(args, " "), detail)
	}

	okCodes := opts.okCodes
	if len(okCodes) == 0 {
		okCodes = []int{0}
	}
	if containsInt(okCodes, result.ExitCode) {
		return result, nil
	}

	detail := strings.TrimSpace(result.Stderr)
	if detail == "" {
		detail = strings.TrimSpace(result.Stdout)
	}
	if detail == "" {
		detail = fmt.Sprintf("exit code %d", result.ExitCode)
	}
	return RunResult{}, fmt.Errorf("command failed: %s :: %s", strings.Join(args, " "), detail)
}

// runRedactedCommand executes a command whose argv contains caller-supplied
// data. Unlike runCommand, it intentionally never returns the argv, stdout,
// stderr, or runner error because any of them can echo that value. Callers
// must pass a fixed, public operation label.
func runRedactedCommand(ctx context.Context, runner Runner, args []string, opts runOptions, operation string) (RunResult, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if opts.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}

	result, err := runner.Run(runCtx, args, opts.input)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return RunResult{}, fmt.Errorf("%s failed: timed out after %dms", operation, opts.timeout.Milliseconds())
		}
		return RunResult{}, fmt.Errorf("%s failed", operation)
	}

	okCodes := opts.okCodes
	if len(okCodes) == 0 {
		okCodes = []int{0}
	}
	if containsInt(okCodes, result.ExitCode) {
		return result, nil
	}
	return RunResult{}, fmt.Errorf("%s failed with exit code %d", operation, result.ExitCode)
}

func runProbe(ctx context.Context, runner Runner, args []string, opts runOptions, failOnError bool) (*RunResult, error) {
	runCtx := ctx
	var cancel context.CancelFunc
	if opts.timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, opts.timeout)
		defer cancel()
	}

	result, err := runner.Run(runCtx, args, opts.input)
	if err != nil {
		if !failOnError {
			return nil, nil
		}
		detail := err.Error()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			detail = fmt.Sprintf("timed out after %dms", opts.timeout.Milliseconds())
		}
		return nil, fmt.Errorf("command failed: %s :: %s", strings.Join(args, " "), detail)
	}
	return &result, nil
}

func dedupe(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out
}

func parseAddress(address string) (waypost.ParsedAddress, error) {
	return waypost.ParseAddress(address)
}

func notificationRouteForAddress(address string) (notificationRoute, error) {
	parsed, err := parseAddress(address)
	if err != nil {
		return notificationRoute{}, err
	}
	return notificationRoute{
		Manager: parsed.Scheme,
		Target:  parsed.ID,
	}, nil
}

func agentDeckAddress(sessionID string) string {
	return "agent-deck/" + sessionID
}

func toolSessionAddress(scheme, sessionID string) string {
	return scheme + "/" + sessionID
}

func boundStateMap(bound boundState) map[string]any {
	out := map[string]any{
		"bound_addresses":                 bound.BoundAddresses,
		"default_sender":                  nilIfEmpty(bound.DefaultSender),
		"default_workdir":                 nilIfEmpty(bound.DefaultWorkdir),
		"detected_agent_deck_session_id":  nilIfEmpty(bound.DetectedAgentDeckSession),
		"detected_thurbox_session_id":     nilIfEmpty(bound.DetectedThurboxSession),
		"detected_tool_session_addresses": bound.DetectedToolSessionAddresses,
		"warnings":                        bound.Warnings,
	}
	for key, value := range detectedToolSessionOutputFields(bound.DetectedToolSessions, nilIfEmpty) {
		out[key] = value
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func nilIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func orUnset(value string) string {
	if strings.TrimSpace(value) == "" {
		return unsetValue
	}
	return value
}

func containsInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
