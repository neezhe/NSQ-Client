package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount  int64 `json:"max_rdy_count"` //这玩意是nsqd端配置的，每个连接不能超过 nsqd 配置 max_rdy_count 。
	TLSv1        bool  `json:"tls_v1"`
	Deflate      bool  `json:"deflate"`
	Snappy       bool  `json:"snappy"`
	AuthRequired bool  `json:"auth_required"`
}

// AuthResponse represents the metadata
// returned from an AUTH command to nsqd
type AuthResponse struct {
	Identity        string `json:"identity"`
	IdentityUrl     string `json:"identity_url"`
	PermissionCount int64  `json:"permission_count"`
}

type msgResponse struct {
	msg     *Message
	cmd     *Command
	success bool
	backoff bool
}

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
type Conn struct {
	//全部是私有变量
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64 //来一个消息就加1，表示正在处理的消息数量
	maxRdyCount      int64 //当前connection可接收的消息数量的最大值（是配置的时候设的个固定值），每个连接这个值都是写死的。
	rdyCount         int64 //用来标识当前connection 还可接收消息的容量，如果收到一个消息就对其减 1。这个值就是告知给nsqd的值。
	lastRdyTimestamp int64
	lastMsgTimestamp int64

	mtx sync.Mutex

	config *Config

	conn    *net.TCPConn
	tlsConn *tls.Conn
	addr    string

	delegate ConnDelegate

	logger   logger
	logLvl   LogLevel
	logFmt   string
	logGuard sync.RWMutex

	r io.Reader
	w io.Writer

	cmdChan         chan *Command
	msgResponseChan chan *msgResponse
	exitChan        chan int
	drainReady      chan int

	closeFlag int32
	stopper   sync.Once
	wg        sync.WaitGroup

	readLoopRunning int32
}

// NewConn returns a new Conn instance
func NewConn(addr string, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr: addr, //服务器地址

		config:   config,   //基本的服务端配置
		delegate: delegate, //producer 与 consumer 用来处理消息回调的接口

		maxRdyCount:      2500, //每个连接这个值都是写死的。
		lastMsgTimestamp: time.Now().UnixNano(), //上一条消息抵达的时间

		cmdChan:         make(chan *Command),     //与服务器的 cmd 管道
		msgResponseChan: make(chan *msgResponse), //消息回应管道
		exitChan:        make(chan int),          //退出信号
		drainReady:      make(chan int),          //中断信号
	}
}

// SetLogger assigns the logger to use as well as a level.
//
// The format parameter is expected to be a printf compatible string with
// a single %s argument.  This is useful if you want to provide additional
// context to the log messages that the connection will print, the default
// is '(%s)'.
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *Conn) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logger = l
	c.logLvl = lvl
	c.logFmt = format
	if c.logFmt == "" {
		c.logFmt = "(%s)"
	}
}

func (c *Conn) getLogger() (logger, LogLevel, string) {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger, c.logLvl, c.logFmt
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	dialer := &net.Dialer{
		LocalAddr: c.config.LocalAddr,
		Timeout:   c.config.DialTimeout,
	}

	conn, err := dialer.Dial("tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn.(*net.TCPConn)
	c.r = conn
	c.w = conn

	_, err = c.Write(MagicV2) //建立连接后就发送MagicV2，下面不等返回就又发送“IDENTIFY”命令，会等待返回
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
	}

	resp, err := c.identify() //告诉服务器自己这边的一些配置,如 clientid, hostname, 心跳间隔时长，超时时长等一些配置信息
	if err != nil {
		return nil, err
	}

	if resp != nil && resp.AuthRequired { //检查服务端是否要求发送请求是否需要授权
		if c.config.AuthSecret == "" {
			c.log(LogLevelError, "Auth Required")
			return nil, errors.New("Auth Required")
		}
		//如果IDENTIFY响应中有auth_required=true，客户端必须在 SUB, PUB 或 MPUB 命令前前发送 AUTH 。否则，客户端不需要认证。
		err := c.auth(c.config.AuthSecret) //发送授权命令
		if err != nil {
			c.log(LogLevelError, "Auth Failed %s", err)
			return nil, err
		}
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1) //纯粹的只是用来表示有readLoop在运行
	//以下与服务端进行通信
	go c.readLoop()//此处读到nsqd发过来的消息并处理之后，下面writeLoop可能需要针对这个消息向nsqd返回指令，指令可以是 REQ，TOUCH，FIN。
	go c.writeLoop()
	return resp, nil
}

// Close idempotently initiates connection close
func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil && atomic.LoadInt64(&c.messagesInFlight) == 0 {
		return c.conn.CloseRead()
	}
	return nil
}

// IsClosing indicates whether or not the
// connection is currently in the processing of
// gracefully closing
func (c *Conn) IsClosing() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

// RDY returns the current RDY count
func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// LastRDY returns the previously set RDY count
func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// SetRDY stores the specified RDY count
func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	if rdy > 0 {
		atomic.StoreInt64(&c.lastRdyTimestamp, time.Now().UnixNano())
	}
}

// MaxRDY returns the nsqd negotiated maximum
// RDY count that it will accept for this connection
func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount //最大的RDY值，就事客户端所能处理的最大消息数
}

// LastRdyTime returns the time of the last non-zero RDY
// update for this connection
func (c *Conn) LastRdyTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastRdyTimestamp)) //lastRdyTimestamp 是这个 connection 上一次调用 SetRdyCount 的时间。
}

// LastMessageTime returns a time.Time representing
// the time at which the last message was received
func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

// RemoteAddr returns the configured destination nsqd address
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// String returns the fully-qualified address
func (c *Conn) String() string {
	return c.addr
}

// Read performs a deadlined read on the underlying TCP connection
func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

// Write performs a deadlined write on the underlying TCP connection
func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
func (c *Conn) WriteCommand(cmd *Command) error {
	c.mtx.Lock()

	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	err = c.Flush()

exit:
	c.mtx.Unlock()
	if err != nil {
		c.log(LogLevelError, "IO error - %s", err)
		c.delegate.OnIOError(c, err)
	}
	return err
}

type flusher interface {
	Flush() error
}

// Flush writes all buffered data to the underlying TCP connection
func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["client_id"] = c.config.ClientID
	ci["hostname"] = c.config.Hostname
	ci["user_agent"] = c.config.UserAgent
	ci["short_id"] = c.config.ClientID // deprecated
	ci["long_id"] = c.config.Hostname  // deprecated
	ci["tls_v1"] = c.config.TlsV1      //允许 TLS 来连接
	ci["deflate"] = c.config.Deflate
	ci["deflate_level"] = c.config.DeflateLevel
	ci["snappy"] = c.config.Snappy //允许 snappy 压缩这次连接
	ci["feature_negotiation"] = true
	if c.config.HeartbeatInterval == -1 {
		ci["heartbeat_interval"] = -1
	} else {
		ci["heartbeat_interval"] = int64(c.config.HeartbeatInterval / time.Millisecond)
	}
	ci["sample_rate"] = c.config.SampleRate
	ci["output_buffer_size"] = c.config.OutputBufferSize // 当 nsqd 写到这个客户端时将会用到的缓存的大小（字节数）
	if c.config.OutputBufferTimeout == -1 {
		ci["output_buffer_timeout"] = -1
	} else {
		//Consumer 有个配置项: OutputBufferTimeout，这个是设置在 flushing 到客户端前，最长的配置时间间隔。而默认是250ms, 配合OutputBufferTimeout的缓冲区，所以消息会有延迟。
		ci["output_buffer_timeout"] = int64(c.config.OutputBufferTimeout / time.Millisecond) //超时后，nsqd 缓冲的数据都会刷新到此客户端。
	}
	ci["msg_timeout"] = int64(c.config.MsgTimeout / time.Millisecond)
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	// check to see if the server was able to respond w/ capabilities
	// i.e. it was a JSON response
	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{} //
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	c.log(LogLevelDebug, "IDENTIFY response: %+v", resp)
	//每个 nsqd 配置了一个参数 max-rdy-count, 如果消费者发出的 RDY 计数， 超出了这个值，这个连接就会关闭。 为了向后兼容，不支持协商功能的 nsqd 这个值假定为2500.
	c.maxRdyCount = resp.MaxRdyCount //客户端会在identify的返回值传入一个 MaxRdyCount,表示我客户端一次最大可以接收这么多消息，服务端对每个 connection 发送的消息永远不超过这个数字。

	if resp.TLSv1 {
		c.log(LogLevelInfo, "upgrading to TLS")
		err := c.upgradeTLS(c.config.TlsConfig)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Deflate {
		c.log(LogLevelInfo, "upgrading to Deflate")
		err := c.upgradeDeflate(c.config.DeflateLevel)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Snappy {
		c.log(LogLevelInfo, "upgrading to Snappy")
		err := c.upgradeSnappy()
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	// now that connection is bootstrapped, enable read buffering
	// (and write buffering if it's not already capable of Flush())
	c.r = bufio.NewReader(c.r)
	if _, ok := c.w.(flusher); !ok {
		c.w = bufio.NewWriter(c.w)
	}

	return resp, nil
}

func (c *Conn) upgradeTLS(tlsConf *tls.Config) error {
	// create a local copy of the config to set ServerName for this connection
	var conf tls.Config
	if tlsConf != nil {
		conf = *tlsConf
	}
	host, _, err := net.SplitHostPort(c.addr)
	if err != nil {
		return err
	}
	conf.ServerName = host

	c.tlsConn = tls.Client(c.conn, &conf)
	err = c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = c.tlsConn
	c.w = c.tlsConn
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *Conn) upgradeDeflate(level int) error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	fw, _ := flate.NewWriter(conn, level)
	c.r = flate.NewReader(conn)
	c.w = fw
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

func (c *Conn) upgradeSnappy() error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = snappy.NewReader(conn)
	c.w = snappy.NewWriter(conn)
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}

func (c *Conn) auth(secret string) error {
	cmd, err := Auth(secret)
	if err != nil {
		return err
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return err
	}

	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}

	if frameType == FrameTypeError {
		return errors.New("Error authenticating " + string(data))
	}

	resp := &AuthResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return err
	}

	c.log(LogLevelInfo, "Auth accepted. Identity: %q %s Permissions: %d",
		resp.Identity, resp.IdentityUrl, resp.PermissionCount)

	return nil
}

func (c *Conn) readLoop() {
	delegate := &connMessageDelegate{c} //是Conn的代理
	for {
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}

		frameType, data, err := ReadUnpackedResponse(c) //阻塞着从Conn读服务器发功来的数据
		if err != nil {
			if err == io.EOF && atomic.LoadInt32(&c.closeFlag) == 1 { //服务端发送FIN时才会read到EOF
				goto exit
			}
			if !strings.Contains(err.Error(), "use of closed network connection") {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}

		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) { //如果读到的是心跳
			c.log(LogLevelDebug, "heartbeat received")
			c.delegate.OnHeartbeat(c)    //不会做任何事
			err := c.WriteCommand(Nop()) //消息是 heartbeat会回复一条空指令告诉服务器自己的存活状态
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			continue //若读到的是心跳则重新读
		}

		switch frameType {
		case FrameTypeResponse:
			c.delegate.OnResponse(c, data) //若读到的FrameTypeResponse不是心跳而是CLOSE_WAIT命令，则关闭连接。对于FrameTypeResponse类型，除了心跳和CLOSE_WAIT，其他的不做处理。
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.Delegate = delegate //把代理也传进去了，表示是这个网络连接上面的消息，后面操作这个消息的时候可以调用这连接的处理方法。
			msg.NSQDAddress = c.String()

			atomic.AddInt64(&c.messagesInFlight, 1) //来一个待压入channel的消息就加1，后面在处理完一个消息后这个变量就会减1
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())

			c.delegate.OnMessage(c, msg) //读到消息，其实调用的就是consumer的onConnMessage函数，把 message 发送到 incomingMessages 管道里面
		case FrameTypeError:
			c.log(LogLevelError, "protocol error - %s", data)
			c.delegate.OnError(c, data)
		default:
			c.log(LogLevelError, "IO error - %s", err)
			c.delegate.OnIOError(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}

exit:
	atomic.StoreInt32(&c.readLoopRunning, 0)
	// start the connection close
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		// if we exited readLoop with no messages in flight
		// we need to explicitly trigger the close because
		// writeLoop won't
		c.close()
	} else {
		c.log(LogLevelWarning, "delaying close, %d outstanding messages", messagesInFlight)
	}
	c.wg.Done()
	c.log(LogLevelInfo, "readLoop exiting")
}

//writeloop 主要关心的是发送 cmd 指令，和回复消息处理的状态，在 nsq客户端中一个消息的处理状态有四种。分别是：
//1.FIN 处理成功，告诉服务端可以放心的丢弃掉这条消息。
//2.REQ 处理失败，告诉服务端这条消息需要重新入队。
//3.TOUCH 告诉服务端需要更多的时间处理这条消息。
//4.消息处理超时，服务端会根据消息处理时长判断消息是否需要重新入队。
func (c *Conn) writeLoop() {
	for {
		select { //循环往nsqd写
		case <-c.exitChan:
			c.log(LogLevelInfo, "breaking out of writeLoop")
			// Indicate drainReady because we will not pull any more off msgResponseChan
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan: //cmdChan只是来自于touch，TOUCH告诉服务端需要更多的时间处理这条消息。
			err := c.WriteCommand(cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", cmd, err)
				c.close()
				continue
			}
		case resp := <-c.msgResponseChan:
			//这里为了消息处理更高效，使用了一个单独的 channel 发送 FIN 和 REQ 状态指令。
			// Decrement this here so it is correct even if we can't respond to nsqd
			msgsInFlight := atomic.AddInt64(&c.messagesInFlight, -1) //回应一个消息，那么InFlight的消息就减1

			if resp.success { //如果成功比表示这个消息已经finish
				c.log(LogLevelDebug, "FIN %s", resp.msg.ID)
				c.delegate.OnMessageFinished(c, resp.msg)
				c.delegate.OnResume(c)
			} else { //如果不成功就要重新入队
				c.log(LogLevelDebug, "REQ %s", resp.msg.ID)
				c.delegate.OnMessageRequeued(c, resp.msg)
				if resp.backoff { //原来backoff是根据消息的返回值来设置的
					c.delegate.OnBackoff(c)
				} else {
					c.delegate.OnContinue(c)
				}
			}

			err := c.WriteCommand(resp.cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", resp.cmd, err)
				c.close()
				continue
			}

			if msgsInFlight == 0 &&
				atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	c.wg.Done()
	c.log(LogLevelInfo, "writeLoop exiting")
}

func (c *Conn) close() {
	// a "clean" connection close is orchestrated as follows:
	//
	//     1. CLOSE cmd sent to nsqd
	//     2. CLOSE_WAIT response received from nsqd
	//     3. set c.closeFlag
	//     4. readLoop() exits
	//         a. if messages-in-flight > 0 delay close()
	//             i. writeLoop() continues receiving on c.msgResponseChan chan
	//                 x. when messages-in-flight == 0 call close()
	//         b. else call close() immediately
	//     5. c.exitChan close
	//         a. writeLoop() exits
	//             i. c.drainReady close
	//     6a. launch cleanup() goroutine (we're racing with intraprocess
	//        routed messages, see comments below)
	//         a. wait on c.drainReady
	//         b. loop and receive on c.msgResponseChan chan
	//            until messages-in-flight == 0
	//            i. ensure that readLoop has exited
	//     6b. launch waitForCleanup() goroutine
	//         b. wait on waitgroup (covers readLoop() and writeLoop()
	//            and cleanup goroutine)
	//         c. underlying TCP connection close
	//         d. trigger Delegate OnClose()
	//
	c.stopper.Do(func() {
		c.log(LogLevelInfo, "beginning close")
		close(c.exitChan)
		c.conn.CloseRead()

		c.wg.Add(1)
		go c.cleanup()

		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	lastWarning := time.Now()
	// writeLoop has exited, drain any remaining in flight messages
	for {
		// we're racing with readLoop which potentially has a message
		// for handling so infinitely loop until messagesInFlight == 0
		// and readLoop has exited
		var msgsInFlight int64
		select {
		case <-c.msgResponseChan:
			msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
		case <-ticker.C:
			msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
		}
		if msgsInFlight > 0 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... waiting for %d messages in flight", msgsInFlight)
				lastWarning = time.Now()
			}
			continue
		}
		// until the readLoop has exited we cannot be sure that there
		// still won't be a race
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... readLoop still running")
				lastWarning = time.Now()
			}
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	c.wg.Done()
	c.log(LogLevelInfo, "finished draining, cleanup exiting")
}

func (c *Conn) waitForCleanup() {
	// this blocks until readLoop and writeLoop
	// (and cleanup goroutine above) have exited
	c.wg.Wait()
	c.conn.CloseWrite()
	c.log(LogLevelInfo, "clean close complete")
	c.delegate.OnClose(c)
}

func (c *Conn) onMessageFinish(m *Message) {
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Finish(m.ID), success: true}
}

func (c *Conn) onMessageRequeue(m *Message, delay time.Duration, backoff bool) {
	if delay == -1 {
		// linear delay
		delay = c.config.DefaultRequeueDelay * time.Duration(m.Attempts)
		// bound the requeueDelay to configured max
		if delay > c.config.MaxRequeueDelay {
			delay = c.config.MaxRequeueDelay
		}
	}
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Requeue(m.ID, delay), success: false, backoff: backoff}
}

func (c *Conn) onMessageTouch(m *Message) {
	select {
	case c.cmdChan <- Touch(m.ID):
	case <-c.exitChan:
	}
}

func (c *Conn) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl, logFmt := c.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl,
		fmt.Sprintf(logFmt, c.String()),
		fmt.Sprintf(line, args...)))
}
