package main

import(
  "context"
  "log/slog"
  "net/http"
  _ "net/http/pprof"
  "os"
  "os/signal"
  "syscall"
  "time"

  "github.com/Akramzg/flowgate/config"
  "github.com/Akramzg/flowgate/internal/proxy"
  "github.com/Akramzg/flowgate/internal/transport"
  "github.com/Akramzg/flowgate/internal/upstream"
)

func main(){
  logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
  }))

  cfg, err := config.Load()
  if err != nil{
    logger.Error("invalid config","err",err)
    os.Exit(1);
  }

  upClient := upstream.NewClient(cfg, logger)

  broker := proxy.NewBroker(cfg, upClient, logger)

  mux := http.NewServeMux()
  mux.Handle("/debug/pprof/", http.DefaultServeMux)

  h := transport.NewHandler(broker, cfg, logger)
  h.Register(mux)

  ln, err := transport.NewListener(cfg.ListenAddr)
  if err != nil{
    logger.Error("failed to create listener", "err", err)
    os.Exit(1)
  }

  server := &http.Server{
    Handler: mux, 
    ReadTimeout: 10 * time.Second,
    IdleTimeout: 90*time.Second,
  }

  go func(){
    logger.Info("pprof listening", "addr", cfg.PProfAddr)
    if err := http.ListenAndServe(cfg.PProfAddr, nil); err != nil{
      logger.Error("pprof server error","err",err)
    }
  }()

  ctx, cancel := context.WithCancel(context.Background())

  go upClient.Run(ctx)
  go broker.Run(ctx)

  go func(){
    logger.Info("flowgate listening","addr",cfg.ListenAddr)
    if err := server.Serve(ln); err != nil && err != http.ErrServerClosed{
      logger.Error("server error","err",err)
      os.Exit(1)
    }
  }()

  sigCh := make(chan os.Signal, 1)
  signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
  <-sigCh

  logger.Info("shutting down...")
  cancel()

  shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer shutdownCancel()

  if err := server.Shutdown(shutdownCtx); err != nil{
    logger.Error("shutdown error","err",err)
  }

  logger.Info("stopped")
}
