package config

import(
  "fmt"
  "os"
  "strconv"
  "time"
)

type Config struct{
  //server
  ListenAddr string

  //upstream
  UpstreamURL string
  UpstreamRetryMin time.Duration
  UpstreamRetryMax time.Duration
  UpstreamRetryMult float64

  //per client buffer
  ClientBufferSize int
  HighWaterRatio float64
  WriteDeadline time.Duration
  SlowClientDelay time.Duration

  //observability
  OTelEndpoint string
  PProfAddr string

}

func Load() (*Config, error){
  cfg := &Config{
    ListenAddr: envStr("LISTEN_ADDR", ":8080"),
    UpstreamURL: envStr("UPSTREAM_URL", ""),
    UpstreamRetryMin: envDuration("UPSTREAM_RETRY_MIN", 500*time.Millisecond),
    UpstreamRetryMax: envDuration("UPSTREAM_RETRY_MAX", 30*time.Millisecond),
    UpstreamRetryMult: envFloat("UPSTREAM_RETRY_MULT", 2.0),
    ClientBufferSize: envInt("CLIENT_BUFFER_SIZE",256),
    HighWaterRatio: envFloat("HIGH_WATER_RATIO", 0.80),
    WriteDeadline:     envDuration("WRITE_DEADLINE", 5*time.Second),
		SlowClientDelay:   envDuration("SLOW_CLIENT_DELAY", 50*time.Millisecond),
		OTelEndpoint:      envStr("OTEL_ENDPOINT", ""),
		PProfAddr:         envStr("PPROF_ADDR", ":6060"),
  }

  if cfg.UpstreamURL == ""{
    return nil, fmt.Errorf("UPSTREAM_URL is required")
  }
  if cfg.ClientBufferSize < 16{
    return nil, fmt.Errorf("CLIENT_BUFFER_SIZE must be >= 16")
  }
  if cfg.HighWaterRatio <= 0 || cfg.HighWaterRatio >=1.0{
    return nil, fmt.Errorf("HIGH_WATER_RATIO must be between 0 and 1")
  }
  return cfg, nil
}

func envStr(key, def string) string{
  if v := os.Getenv(key); v != ""{
    return v
  }
  return def
}


func envInt(key string, def int) int{
  if v := os.Getenv(key); v!=""{
    if n, err := strconv.Atoi(v); err ==nil{
      return n
    }
  }
  return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
    if f, err := strconv.ParseFloat(v,64); err == nil{
      return f
    }
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
    if d, err := time.ParseDuration(v); err ==nil{
      return d
    }
	}
	return def
}
