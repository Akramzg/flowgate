package upstream


import(
  "bufio"
  "context"
  "fmt"
  "io"
  "log/slog"
  "math"
  "net/http"
  "strings"
  "time"

  "github.com/Akramzg/flowgate/config"
)

type Client struct{
  cfg *config.Config
  out chan Event
  logger *slog.Logger
}

func NewClient(cfg *config.Config, logger *slog.Logger) *Client{
  return &Client{
    cfg: cfg,
    out: make(chan Event,512),
    logger: logger,
  }
}

func (c *Client) Event() <-chan Event{
  return c.out
}

func (c *Client) Run(ctx context.Context){
  backoff := c.cfg.UpstreamRetryMin
  for{
    if err := c.connect(ctx); err != nil{
      if ctx.Err() != nil{
        return
      }

      c.logger.Warn("upstream disconnected","err",err,"retry_in",backoff,)
      select{
      case <-time.After(backoff):
      case <-ctx.Done():
        return
      }

      backoff = time.Duration(math.Min(
        float64(backoff)*c.cfg.UpstreamRetryMult,
        float64(c.cfg.UpstreamRetryMax),
        ))
      continue
    }

    backoff = c.cfg.UpstreamRetryMin
  }
}


func (c *Client) connect(ctx context.Context) error{
  req, err := http.NewRequestWithContext(ctx,http.MethodGet,c.cfg.UpstreamURL,nil)
  if err != nil{
    return fmt.Errorf("build request: %w",err)
  }

  req.Header.Set("Accept","text/event-stream")
  req.Header.Set("Cache-Control","no-cache")
  resp,err := http.DefaultClient.Do(req)
  if err != nil{
    return fmt.Errorf("connect: %w",err)
  }
  defer resp.Body.Close()

  if resp.StatusCode != http.StatusOK{
    return fmt.Errorf("upstream returned %d",resp.StatusCode)
  }

  c.logger.Info("upstream connected","url",c.cfg.UpstreamURL)
  return c.stream(ctx,resp.Body)
}

func (c *Client) stream(ctx context.Context, body io.Reader) error{
  scanner := bufio.NewScanner(body)
  var lines []string
  
  for scanner.Scan(){
    if ctx.Err() != nil{
      return nil
    }
    line := scanner.Text()
    if line == ""{
      raw := strings.Join(lines, "\n") + "\n"
      lines = lines[:0]

      events := ParseEvents(raw)
      for _,ev := range events{
        select{
        case c.out <-ev:
        case <-ctx.Done():
          return nil
        }
      }
      continue
    }
    lines = append(lines, line)
  }
  if err := scanner.Err(); err != nil{
    return fmt.Errorf("read upstream: %w",err)
  }

  return io.EOF
}
