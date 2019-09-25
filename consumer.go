package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	consumer.AddHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// DiscoveryFilter is an interface accepted by `SetBehaviorDelegate()`
// for filtering the nsqds returned from discovery via nsqlookupd
type DiscoveryFilter interface {
	Filter([]string) []string
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

// ConsumerStats represents a snapshot of the state of a Consumer's connections and the messages
// it has seen
type ConsumerStats struct {
	MessagesReceived uint64
	MessagesFinished uint64
	MessagesRequeued uint64
	Connections      int
}

var instCount int64

type backoffSignal int

const (
	backoffFlag backoffSignal = iota
	continueFlag
	resumeFlag
)

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Consumer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64 //来一个消息就加1一次
	messagesFinished uint64 //返回一次FIN命令此处就加1
	messagesRequeued uint64 //消息重新入队就加1
	totalRdyCount    int64  //前消费者所有连接的rdyCount（此字段在conn结构体内）之和，即所有连接的可处理消息的总容量，是动态变化的。
	backoffDuration  int64
	backoffCounter   int32
	maxInFlight      int32 //客户端能够允许的正在处理的消息的最大数量。指消费者每个连接的RDY的计数的总和total_rdy_count不应超过配置的 max_in_flight 。

	mtx sync.RWMutex

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	id      int64
	topic   string
	channel string
	config  Config

	rngMtx sync.Mutex
	rng    *rand.Rand

	needRDYRedistributed int32

	backoffMtx sync.Mutex

	incomingMessages chan *Message

	rdyRetryMtx    sync.Mutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]*Conn
	connections        map[string]*Conn //“nsqd的ip:port”为key,value为连接套接字为Conn,意思就是说一个消费者可能和多个不同addr的nsqd建立了连接

	nsqdTCPAddrs []string

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once
	exitHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	config.assertInitialized()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		logger:      log.New(os.Stderr, "", log.Flags()),
		logLvl:      LogLevelInfo,
		maxInFlight: 3, //int32(config.MaxInFlight), //此值在后续不会发生变化，若此值设置成1(config中此处默认设置的是1)，就不会启用缓冲区，但是这样就需要前一条消息ACK后才处理下一条，所以不可取。(这个值其实可以理解成客户端装那些待处理消息的一块缓冲区的大小)

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]*Conn),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),

		rng: rand.New(rand.NewSource(time.Now().UnixNano())),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}
	r.wg.Add(1)
	//nsqd的客户端在连接nsqd的时候就会设置一个初始的rdycount 。当然，在连接成功之后，也会有一个gorountine 后台不断去调整这个rdycount
	go r.rdyLoop() //定时更新RDY的值,由于一个消费者可以连接到多个不同地址的nsqd,所以此处就调整这个消费者所有连接上的rdy值。
	return r, nil
}

// Stats retrieves the current connection and message statistics for a Consumer
func (r *Consumer) Stats() *ConsumerStats {
	return &ConsumerStats{
		MessagesReceived: atomic.LoadUint64(&r.messagesReceived),
		MessagesFinished: atomic.LoadUint64(&r.messagesFinished),
		MessagesRequeued: atomic.LoadUint64(&r.messagesRequeued),
		Connections:      len(r.conns()),
	}
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logGuard.Lock()
	defer r.logGuard.Unlock()

	r.logger = l
	r.logLvl = lvl
}

func (r *Consumer) getLogger() (logger, LogLevel) {
	r.logGuard.RLock()
	defer r.logGuard.RUnlock()

	return r.logger, r.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//    DiscoveryFilter
//
func (r *Consumer) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	r.behaviorDelegate = cb
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible for.
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns())) //能处理消息的最大数量平均到每个连接上的数量，比如大小为3的MaxInFlight平均到2个连接上，每个上面就是1
	//1.当MaxInFlight小于等于连接数时，返回值为1。其实等于的情况是可以刚刚分配合理。但是小于的情况下，有些连接是永远无法收到消息的，如何处理？
	//2.当MaxInFlight大于连接数时，返回值为b / float64(len(r.conns())，可为1可大于1
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(conn.RDY()) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.maybeUpdateRDY(c) //这表示每当处理一条消息，都有可能重新进行并发控制计算。
	}
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 { //判断消费者是否停止
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 { //这个消费者是否有hanler在工作
		return errors.New("no handlers")
	}

	if err := validatedLookupAddr(addr); err != nil { //是否是书写合法的ip地址
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1) //把此消费者标记为连接状态

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs { //遍历lookupdHTTPAddrs，看是否这个地址已经添加过了，如果添加过了就直接返回，否则将当前addr地址添加到lookupdHTTPAddrs中
		if x == addr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, addr)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 { //如果现在lookupdHTTPAddrs的长度是1，说明没有启动过lookupdLoop。下面的代码只会在第一个lookupdHTTPAddr时执行
		r.queryLookupd() //开始访问链接http://127.0.0.1:4161/lookup?topic=lizhe，拿到这个topic所在nsqd的地址
		r.wg.Add(1)
		go r.lookupdLoop() //在这里面会定时向nsqlookupd发送http请求，更新与nsqd的连接，当有新的nsqd负责topic的存储的时候可以马上向这个nsqd获取消息。
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func validatedLookupAddr(addr string) error {
	if strings.Contains(addr, "/") {
		_, err := url.Parse(addr)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(addr, ":") {
		return errors.New("missing port")
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (r *Consumer) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	r.rngMtx.Lock()
	jitter := time.Duration(int64(r.rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	r.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	ticker = time.NewTicker(r.config.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (r *Consumer) nextLookupdEndpoint() string {
	r.mtx.RLock()
	if r.lookupdQueryIndex >= len(r.lookupdHTTPAddrs) {
		r.lookupdQueryIndex = 0
	}
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}

	v, err := url.ParseQuery(u.RawQuery)
	v.Add("topic", r.topic)
	u.RawQuery = v.Encode()
	return u.String()
}

type lookupResp struct {
	Channels  []string    `json:"channels"`
	Producers []*peerInfo `json:"producers"`
	Timestamp int64       `json:"timestamp"`
}

type peerInfo struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (r *Consumer) queryLookupd() {
	retries := 0

retry:
	endpoint := r.nextLookupdEndpoint() //根据lookupdHTTPAddrs构造请求lookupd的地址。

	r.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data) //请求nsqlookupd获取分配给此消费者的nsqd节点信息,nsqlookupd会向消费者返回存在用户想消费的topic的所有nsqd的地址
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		retries++
		if retries < 3 {
			r.log(LogLevelInfo, "retrying with next nsqlookupd")
			goto retry
		}
		return
	}

	var nsqdAddrs []string

	for _, producer := range data.Producers { //遍历返回的生产者，组装成nsqdAdders

		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
		fmt.Println("=====Producers========", broadcastAddress, port, nsqdAddrs)

	}
	// apply filter
	if discoveryFilter, ok := r.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}
	fmt.Println("=====nsqdAddrs========", nsqdAddrs)
	for _, addr := range nsqdAddrs { //和过滤后的nsqd建立联系
		err = r.ConnectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
	}
}

// ConnectToNSQDs takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
func (r *Consumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQD(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (r *Consumer) ConnectToNSQD(addr string) error {
	//consumer和producer在调用Stop函数的时候会设置此标志位
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	//检查有多少个处理函数协程在跑（只要接受的incomingMessages channel不被关闭，处理协程是不会退出for循环的）
	//由此if语句可见，消费者在连接到nsqd之前必须要通过AddHandler添加处理函数
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	atomic.StoreInt32(&r.connectedFlag, 1) //这玩意表示已经开始发起连接nsqd或者lookupd了，并不代表真的连接上。

	logger, logLvl := r.getLogger()
	// go-nsq/conn.go是对底层连接的一个抽象，他是不关心你是生产者还是消费者，这里使用到了代理模式
	//consumerConnDelegate这个结构体的目的就是把consumer的那一套私有方法暴露出来。Delegate就是委托代表的意思，此处是consumer的委托。
	//此处表明，我第3个参数要开始代理你这个消费者了。
	conn := NewConn(addr, &r.config, &consumerConnDelegate{r}) //producer与consumer会在调用 NewConn函数时传入一个自己的delegate用来给connection模块回调处理消息
	conn.SetLogger(logger, logLvl,
		fmt.Sprintf("%3d [%s/%s] (%%s)", r.id, r.topic, r.channel))

	//并没有马上建立连接，先从pendingConnections和connections中尝试获取addr对应的conn，如果获取到了，
	// 说明建立过连接了，直接返回，否则先添加到pendingConnections中，创建了一个匿名函数cleanupConnection，
	// 当连接建立失败后进行清理工作，之后才正式建立连接。
	r.mtx.Lock()
	_, pendingOk := r.pendingConnections[addr] //一个addr对应一个nsqd,那么此处表示一个消费者可能有多个nsqd
	_, ok := r.connections[addr]
	if ok || pendingOk {
		r.mtx.Unlock()
		return ErrAlreadyConnected
	}
	r.pendingConnections[addr] = conn //pendingConnections在执行Connect()函数之前添加数据，connections在执行Connect()函数之后添加数据
	if idx := indexOf(addr, r.nsqdTCPAddrs); idx == -1 {
		r.nsqdTCPAddrs = append(r.nsqdTCPAddrs, addr) //nsqdTCPAddrs里面专门存这个客户端要连接的所有nsqd的地址
	}
	r.mtx.Unlock()

	r.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr) //delete是专门删map中的元素的。
		r.mtx.Unlock()
		conn.Close()
	}

	resp, err := conn.Connect() //producer 和 consumer 会调用 Connect() 函数与 nsq 建立连接
	if err != nil { //若连接出错
		cleanupConnection()
		return err
	}
	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) { //客户端能处理的最大值若小于传输中的消息允许的最大数量，则报错
			r.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}
	//如果成功建立一个订阅命令，通过conn向当前的nsqd发送过去，
	//更新pendingConnections和connections，最后检查当前consumer的所有conn是否有必要更新RDY的值
	cmd := Subscribe(r.topic, r.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, r.topic, r.channel, err.Error())
	}

	r.mtx.Lock()
	delete(r.pendingConnections, addr)
	r.connections[addr] = conn //也就是说上面的连接都成功了后，这个conn会被记录到这个connsumer的connections中去。
	r.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count，
	//有新的连接建立了后，那么肯定就要降低之前连接的RDY数。
	//RDY 是 nsqd 推送消息流量控制的核心。会在创建连接时、接受到消息时触发，里面有 3 个情况会发送 RDY 指令。
	//1.剩余量太少
	//2.消费得很快
	//3.超量了
	for _, c := range r.conns() { //会遍历这个Consumer的所有nsqd conn（Consumer可以同时连接多个nsqd），然后调用	maybeUpdateRDY 这个方法
		//当客户端连接到 nsqd 和并订阅到一个通道时，它被放置在一个 RDY为0状态。这意味着，还没有信息被发送到客户端。
		//当客户端已准备好接收消息发送，更新它的命令 RDY 状态到它准备处理的数量，比如 100。无需任何额外的指令，当 100条消息可用时，将被传递到客户端
		r.maybeUpdateRDY(c) //这表示每当处理一条消息或者建立一个连接，都有可能重新进行并发控制计算。重新均衡分配，最理想的情况是 MaxInFlight / len(conns)，把RDY计数均衡分布到所有连接。
	}

	return nil
}

func indexOf(n string, h []string) int {
	for i, a := range h {
		if n == a {
			return i
		}
	}
	return -1
}

// DisconnectFromNSQD closes the connection to and removes the specified
// `nsqd` address from the list
func (r *Consumer) DisconnectFromNSQD(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.nsqdTCPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	// slice delete
	r.nsqdTCPAddrs = append(r.nsqdTCPAddrs[:idx], r.nsqdTCPAddrs[idx+1:]...)

	pendingConn, pendingOk := r.pendingConnections[addr]
	conn, ok := r.connections[addr]

	if ok {
		conn.Close()
	} else if pendingOk {
		pendingConn.Close()
	}

	return nil
}

// DisconnectFromNSQLookupd removes the specified `nsqlookupd` address
// from the list used for periodic discovery.
func (r *Consumer) DisconnectFromNSQLookupd(addr string) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	idx := indexOf(addr, r.lookupdHTTPAddrs)
	if idx == -1 {
		return ErrNotConnected
	}

	if len(r.lookupdHTTPAddrs) == 1 {
		return fmt.Errorf("cannot disconnect from only remaining nsqlookupd HTTP address %s", addr)
	}

	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs[:idx], r.lookupdHTTPAddrs[idx+1:]...)

	return nil
}

func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesReceived, 1)
	r.incomingMessages <- msg //把 message发送到incomingMessages管道里面
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
}

func (r *Consumer) onConnBackoff(c *Conn) {
	r.startStopContinueBackoff(c, backoffFlag)
}

func (r *Consumer) onConnContinue(c *Conn) {
	r.startStopContinueBackoff(c, continueFlag)
}

func (r *Consumer) onConnResume(c *Conn) {
	r.startStopContinueBackoff(c, resumeFlag)
}

func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		r.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (r *Consumer) onConnError(c *Conn, data []byte) {}

func (r *Consumer) onConnHeartbeat(c *Conn) {}

func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (r *Consumer) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the consumer's total
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)

	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	r.rdyRetryMtx.Unlock()

	r.mtx.Lock()
	delete(r.connections, c.String())
	left := len(r.connections)
	r.mtx.Unlock()

	r.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == r.getMaxInFlight() || r.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		if left == 0 {
			r.stopHandlers()
		}
		return
	}

	r.mtx.RLock()
	numLookupd := len(r.lookupdHTTPAddrs)
	reconnect := indexOf(c.String(), r.nsqdTCPAddrs) >= 0
	r.mtx.RUnlock()
	if numLookupd > 0 {
		// trigger a poll of the lookupd
		select {
		case r.lookupdRecheckChan <- 1:
		default:
		}
	} else if reconnect {
		// there are no lookupd and we still have this nsqd TCP address in our list...
		// try to reconnect after a bit
		go func(addr string) {
			for {
				r.log(LogLevelInfo, "(%s) re-connecting in %s", addr, r.config.LookupdPollInterval)
				time.Sleep(r.config.LookupdPollInterval)
				if atomic.LoadInt32(&r.stopFlag) == 1 {
					break
				}
				r.mtx.RLock()
				reconnect := indexOf(addr, r.nsqdTCPAddrs) >= 0
				r.mtx.RUnlock()
				if !reconnect {
					r.log(LogLevelWarning, "(%s) skipped reconnect after removal...", addr)
					return
				}
				err := r.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.String())
	}
}

func (r *Consumer) startStopContinueBackoff(conn *Conn, signal backoffSignal) {
	// prevent many async failures/successes from immediately resulting in
	// max backoff/normal rate (by ensuring that we dont continually incr/decr
	// the counter during a backoff period)
	r.backoffMtx.Lock()
	defer r.backoffMtx.Unlock()
	if r.inBackoffTimeout() {
		return
	}

	// update backoff state
	backoffUpdated := false
	backoffCounter := atomic.LoadInt32(&r.backoffCounter)
	switch signal {
	case resumeFlag:
		if backoffCounter > 0 {
			backoffCounter--
			backoffUpdated = true
		}
	case backoffFlag:
		nextBackoff := r.config.BackoffStrategy.Calculate(int(backoffCounter) + 1)
		if nextBackoff <= r.config.MaxBackoffDuration {
			backoffCounter++
			backoffUpdated = true
		}
	}
	atomic.StoreInt32(&r.backoffCounter, backoffCounter)

	if r.backoffCounter == 0 && backoffUpdated {
		// exit backoff
		count := r.perConnMaxInFlight()
		r.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
		for _, c := range r.conns() {
			r.updateRDY(c, count)
		}
	} else if r.backoffCounter > 0 {
		// start or continue backoff
		backoffDuration := r.config.BackoffStrategy.Calculate(int(backoffCounter))

		if backoffDuration > r.config.MaxBackoffDuration {
			backoffDuration = r.config.MaxBackoffDuration
		}

		r.log(LogLevelWarning, "backing off for %s (backoff level %d), setting all to RDY 0",
			backoffDuration, backoffCounter)

		// send RDY 0 immediately (to *all* connections)
		for _, c := range r.conns() {
			r.updateRDY(c, 0)
		}

		r.backoff(backoffDuration)
	}
}

func (r *Consumer) backoff(d time.Duration) {
	atomic.StoreInt64(&r.backoffDuration, d.Nanoseconds())
	time.AfterFunc(d, r.resume)
}

func (r *Consumer) resume() {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		atomic.StoreInt64(&r.backoffDuration, 0)
		return
	}

	// pick a random connection to test the waters
	conns := r.conns()
	if len(conns) == 0 {
		r.log(LogLevelWarning, "no connection available to resume")
		r.log(LogLevelWarning, "backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}
	r.rngMtx.Lock()
	idx := r.rng.Intn(len(conns))
	r.rngMtx.Unlock()
	choice := conns[idx]

	r.log(LogLevelWarning,
		"(%s) backoff timeout expired, sending RDY 1",
		choice.String())

	// while in backoff only ever let 1 message at a time through
	err := r.updateRDY(choice, 1)
	if err != nil {
		r.log(LogLevelWarning, "(%s) error resuming RDY 1 - %s", choice.String(), err)
		r.log(LogLevelWarning, "backing off for %s", time.Second)
		r.backoff(time.Second)
		return
	}

	atomic.StoreInt64(&r.backoffDuration, 0)
}

func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

func (r *Consumer) inBackoffTimeout() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

func (r *Consumer) maybeUpdateRDY(conn *Conn) {
	//backoff：退避算法或者回退算法，当消息处理失败，接下去应该做什么， 是个很复杂的问题。
	//通过退避算法，消费者允许下游系统从瞬态故障中恢复。 这种行为应该可配置的。因为它并不一定需要，例如在低延迟的环境。
	//当消费者在backoff状态时, 这个消费者将不再处理任何消息, 直到backoff超时.
	//有时候 consumer 处理消息面临很大的压力，随时有崩溃的风险，这种情况下可以主动向 nsq 发送 RDY 0 实现 backoff ，换句话说就是消费端暂停接受等多消息，以减轻自身压力避免崩溃，等到有更多处理能力时再取消暂停状态慢慢接收更多消息。当然进入 backoff 然后慢慢恢复是一个需要动态调节的过程。
	inBackoff := r.inBackoff()
	inBackoffTimeout := r.inBackoffTimeout()
	if inBackoff || inBackoffTimeout { //判断这个消费者是否是在inBackoff或inBackoffTimeout状态
		r.log(LogLevelDebug, "(%s) skip sending RDY inBackoff:%v || inBackoffTimeout:%v",
			conn, inBackoff, inBackoffTimeout)
		return
	}
	//下面拿到的是平均到每个连接的最大rdy值，当连接数大于maxinflight时，则有些连接的rdy可以设置为1，有些就不能设置了,因为都设为1超过了承受范围，这个工作就交给下面updateRDY去筛选。
	//也就是说我此处先拿到每个连接可能下发的rdy值，但是有没有资格下发，就交给updateRDY去判断。
	count := r.perConnMaxInFlight()

	r.log(LogLevelDebug, "(%s) sending RDY %d", conn, count)
	r.updateRDY(conn, count) //此时才开始调整这个消费者在单个连接conn上的RDY count
}

func (r *Consumer) rdyLoop() {
	redistributeTicker := time.NewTicker(r.config.RDYRedistributeInterval)

	for {
		select {
		case <-redistributeTicker.C: //默认 5s
			//maybeUpdateRDY会均衡分配RDY,最理想的情况是 MaxInFlight / len(conns),但是有时候MaxInFlight会小于连接数，这就需要重新分配了，下面的函数就是来做这个事的
			r.redistributeRDY() //每隔5秒此函数会判断MaxInFlight与连接数，若小于连接数，根据评估就会自动让某些链接不发送消息。
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	r.log(LogLevelInfo, "rdyLoop exiting")
	r.wg.Done()
}

func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return ErrClosing
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() { //首先需要确保该值不会超过nsqd设置的最大RDY（nsqd设置的最大RDY值是在与nsqd建立连接时获取到的）
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok { //rdyRetryTimers的更新只在下面965行出现过。
		timer.Stop() //此定时执行停止
		delete(r.rdyRetryTimers, c.String())
	}
	r.rdyRetryMtx.Unlock()

	//所有连接上的RDY之和不能够超过max-in-flight，
	//因此当前连接上能够设置的RDY的最大值，就是max-in-flight减去当前消费者除此连接外所有连接的rdyCount之和，即maxPossibleRdy，使用此值来约束待更新的RDY的上限。
	rdyCount := c.RDY() //当前连接的rdy数，可以理解为maxinflight分给当前连接的缓存大小，totalRdyCount就是这些缓存的总和。
	//比如当maxinflight为1，连接数为2，当走到第一个连接中，maxPossibleRdy为1，count为1，此1可以sendRDY到nsqd，
	//当循环到第二个连接，maxPossibleRdy就变成了0，count为1，此1就无法通过sendRDY发送到nsqd,因为直接return ErrOverMaxInFlight返回了，所以此连接暂时无法与nsqd通信，当然后rdyLoop中会进行调整。
	//MaxInFlight在消费者创建的时候就配置好了的，totalRdyCount和rdyCount则是动态变化的
	maxPossibleRdy := int64(r.getMaxInFlight()) - atomic.LoadInt64(&r.totalRdyCount) + rdyCount //减去totalRdyCount就是减去所有连接的rdyCount之和，加上rdyCount就是减去当前消费者除此连接外所有连接的rdyCount之和。
	if maxPossibleRdy > 0 && maxPossibleRdy < count { //如果传进来的“这个连接的count值”比“这个连接上可能的最大RDY值”都大，一般不会出现这种情况，此处只是以防万一。
		count = maxPossibleRdy
	}
	//进了上面的if语句就不可能进下面的if语句
	//count是绝对大于0的，maxPossibleRdy小于0时，比如MaxInFlight为5，totalRdyCount为7，rdyCount为1
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 { //信息流不足，就要关闭一些RDY为0的连接
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			r.rdyRetryMtx.Lock()
			//这里的key是这个链接的addr，这个定时器会阻塞吗？不会
			r.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second, //这个定时器在上面941行可能会被停止。
				func() {
					r.updateRDY(c, count) //递归,过5秒会执行，这个5秒钟的函数不是周期5秒的执行，只一次。
				})
			r.rdyRetryMtx.Unlock()
		}
		//此处返回，也就是说连接数大于maxInFlight时，这个rdy值不会通过sendRDY设下去，这个连接的rdy就为0
		return ErrOverMaxInFlight //只要进入此if语句，就会返回此错误（表示连接数大于maxInFlight，且每个连接上竟然都是有效的，其rdy在redistributeRDY协程中都不会被设为0），这表示超过了此消费者的最大处理能力。
	}

	return r.sendRDY(c, count) //若count为0，上面所有操作都不会进入。
}

func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 { //注意此处是与的关系
		// no need to send. It's already that RDY count
		return nil
	}
	//更新了totalRdyCount（另外一个更新此值的地方是close的时候），即所有连接的RDY之和(当前连接之外的其他所有连接的RDY之和)，同时更新当前连接的RDY。
	//和rdyCount一样，这个值不会因为消息的到来而发生变化。
	atomic.AddInt64(&r.totalRdyCount, count-c.RDY()) //count是分配给这个连接的rdy容量，c.RDY()表示这个连接多出的rdy容量，相减就是多出的容量，可以为负。每个连接调整rdy数的时候也要更新消费者totalRdyCount
	c.SetRDY(count)                                  //唯一一处设置此连接的rdyCount值，这个值就是传输给nsqd的值。卧槽这个值设置后，不会因为消息到来后会发生变化，这个值就是用来标识这个连接的容量。
	err := c.WriteCommand(Ready(int(count)))         //告诉nsqd你可以向我提供count个消息
	if err != nil {
		r.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

func (r *Consumer) redistributeRDY() {
	if r.inBackoffTimeout() {
		return
	}
	// if an external heuristic set needRDYRedistributed we want to wait
	// until we can actually redistribute to proceed
	conns := r.conns()
	if len(conns) == 0 { //没有连接，所以就谈不上调整rdy了
		return
	}

	maxInFlight := r.getMaxInFlight()
	if len(conns) > int(maxInFlight) { //当连接数大于maxInFlight的时候，收到的消息的数量就会大于maxInFlight，这样就要超出我们客户端的处理要求了，所以就需要进行调整。怎么个调整法呢？
		r.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			len(conns), maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if r.inBackoff() && len(conns) > 1 {
		r.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", len(conns))
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) { //若相等则交换，返回值代表是否交换
		return
	}
	//若len(conns)>0且len(conns) > int(maxInFlight)的情况下，才继续往下走进行rdy的重新分配，不然上一步就return了
	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime()) //LastMessageTime 是上一条消息处理前设定的时间，此处相减
		lastRdyDuration := time.Now().Sub(c.LastRdyTime())     //lastRdyTimestamp 是这个 connection 上一次调用 SetRdyCount 的时间。
		rdyCount := c.RDY()                                    //表示当前连接的rdy数（可接收消息的容量值）
		r.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 { //若这个连接还在接收消息的话，下面就判断是否让其不要接收消息。
			//这两个时间的对比主要是为了找出那些处理消息比较慢的连接，让这些连接别发消息了，尽量让其他优质的connection去接收消息。
			if lastMsgDuration > r.config.LowRdyIdleTimeout {
				r.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
				r.updateRDY(c, 0) //告诉相应连接的nsqd别在此连接上发消息了
			} else if lastRdyDuration > r.config.LowRdyTimeout {
				r.log(LogLevelDebug, "(%s) RDY timeout, giving up RDY", c.String())
				r.updateRDY(c, 0)
			}
		}
		possibleConns = append(possibleConns, c) //仍然是len(conns)个链接，但这样拿到的都是rdyCount为0的链接和优质的链接
	}
	//其实我觉得possibleConns可以在此处先排除掉那些活跃的连接，将availableMaxInFlight分配到possibleConns中rdycount为0的连接上。
	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&r.totalRdyCount) //计算空余可以分配的的 RDY数量.最大值减去目前所有链接的rdy数目之和。
	if r.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&r.totalRdyCount)
	}
	//比如经过上面的调整后，len(possibleConns)为6，maxInFlight为5，totalRdyCount由6变成为3（这是因为把几个完全不通信的链接的rdy值设为了0），availableMaxInFlight为2
	//则下面的for循环会把2分两次（每次1个），随机分配到possibleConns中的6个连接上。
	//这样只要上面每次调整不能使availableMaxInFlight为0，则下面就重新分配。
	//availableMaxInFlight为0的情况就是maxInFlight恰好分配在maxInFlight个活跃的连接上，此时totalRdyCount与maxInFlight相等。
	//若len(conns) > int(maxInFlight)且len(conns)个连接都是活跃的,在updateRDY中会报错的。
	for len(possibleConns) > 0 && availableMaxInFlight > 0 { //重新随机分配空余的 RDY,循环一次len(possibleConns)和availableMaxInFlight都会减1
		availableMaxInFlight--
		r.rngMtx.Lock()
		i := r.rng.Int() % len(possibleConns) //卧槽，这个地方竟然采用的是随机的方式
		r.rngMtx.Unlock()
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		r.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		//其实我觉得possibleConns可以在此处先排除掉那些活跃的连接，将availableMaxInFlight分配到possibleConns中rdycount为0的连接上。
		//if c.rdyCount > 0 { //这个if语句是我加的
		//	continue
		//}
		r.updateRDY(c, 1) //此处为何发送的是1？因为对于len(conns) > int(maxInFlight)的情况，每个连接只能分配1个RDY数。
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	if len(r.conns()) == 0 {
		r.stopHandlers()
	} else {
		for _, c := range r.conns() {
			err := c.WriteCommand(StartClose())
			if err != nil {
				r.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			// if we've waited this long handlers are blocked on processing messages
			// so we can't just stopHandlers (if any adtl. messages were pending processing
			// we would cause a panic on channel close)
			//
			// instead, we just bypass handler closing and skip to the final exit
			r.exit()
		})
	}
}

func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		r.log(LogLevelInfo, "stopping handlers")
		close(r.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumer. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}
	atomic.AddInt32(&r.runningHandlers, int32(concurrency)) //记录这个消费者正在做处理的协程数量
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler) //里面是个死循环。consumer模块内，有一个 handleloop不断轮询incomingMessages管道来接收connection模块发过来的消息。
	}
}

func (r *Consumer) handlerLoop(handler Handler) { //这是个协程
	r.log(LogLevelDebug, "starting Handler")

	for {
		fmt.Println("=======message=init======")
		message, ok := <-r.incomingMessages //会阻塞,不断轮询 incomingMessages 管道来接收 connection 模块发过来的消息。
		if !ok { //这个channel关闭的时候。由此可见当此协程退出的时候，runningHandlers会自动减1
			goto exit
		}
		fmt.Println("=======message=======", string(message.Body))
		if r.shouldFailMessage(message, handler) { //shouldFailMessage只能判断处理重试次数过多的失败
			message.Finish() //发送这个消息的FINISH命令，触发writeLoop协程
			continue
		}

		err := handler.HandleMessage(message) //调用之前注册的handler
		if err != nil {
			r.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1) //触发writeLoop协程
			}
			continue
		}

		if !message.IsAutoResponseDisabled() { //这个消息是否需要自动回复，这个字段在消息里面。
			message.Finish() //完成一条消息
		}
	}

exit:
	r.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

func (r *Consumer) shouldFailMessage(message *Message,
	handler interface{}) bool {
	// message passed the max number of attempts
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		r.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger) //此处只是通过ok来判断handler是否是FailedMessageLogger类型的接口对象，若是则为true
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (r *Consumer) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
		close(r.StopChan)
	})
}

func (r *Consumer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := r.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %3d [%s/%s] %s",
		lvl, r.id, r.topic, r.channel,
		fmt.Sprintf(line, args...)))
}
