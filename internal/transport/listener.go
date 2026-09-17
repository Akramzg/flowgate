package transport

import(
  "net"
  "time"
)

type tcpOptionListener struct{
  net.Listener
}

func NewListener(addr string) (net.Listener, error){
  ln, err := net.Listen("tcp",addr)
  if err != nil{
    return nil, err
  }
  return &tcpOptionListener{ln}, nil
}

func (l *tcpOptionListener) Accept() (net.Conn, error){
  conn, err := l.Listener.Accept()
  if err != nil{
    return nil, err
  }

  if tc, ok := conn.(*net.TCPConn); ok{
    //no 200ms batching, each SSE event is sent immediately
    tc.SetNoDelay(true)
    //detecting dead clients that didn't close cleanly
    tc.SetKeepAlive(true)
    tc.SetKeepAlivePeriod(30*time.Second)
    //smaller send buffer so the kernel backpressure kicks in faster which trigers our app-level backpressure sooner
    tc.SetWriteBuffer(32*1024)
  }
  return conn, nil
}
