package app

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cw3/internal/config"
	"cw3/internal/controller"
	mysqlpkg "cw3/internal/pkg/mysql"
	redispkg "cw3/internal/pkg/redis"
	"cw3/internal/repository"
	"cw3/internal/router"
	"cw3/internal/service"

	"github.com/gin-gonic/gin"
)

type App struct {
	cfg    *config.Config
	redis  *redispkg.Client
	mysql  *mysqlpkg.DB
	server *http.Server
}

func New() (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("load config failed: %w", err)
	}

	gin.SetMode(cfg.Server.Mode)

	redisClient, err := redispkg.New(&cfg.Redis)
	if err != nil {
		return nil, fmt.Errorf("init redis failed: %w", err)
	}
	log.Println("redis connected successfully")

	mysqlClient, err := mysqlpkg.New(&cfg.MySQL)
	if err != nil {
		return nil, fmt.Errorf("init mysql failed: %w", err)
	}
	log.Println("mysql connected successfully")

	redisRepo := repository.NewStreamRedisRepository(redisClient)
	mysqlRepo := repository.NewStreamMySQLRepository(mysqlClient)

	streamService := service.NewStreamService(redisRepo, mysqlRepo)

	streamController := controller.NewStreamController(streamService)

	r := router.SetupRouter(streamController)

	addr := fmt.Sprintf(":%s", cfg.Server.Port)
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	return &App{
		cfg:    cfg,
		redis:  redisClient,
		mysql:  mysqlClient,
		server: server,
	}, nil
}

func (a *App) Run() error {
	go func() {
		log.Printf("server starting on %s", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server listen failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := a.server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
		return fmt.Errorf("server shutdown failed: %w", err)
	}
	log.Println("server shutdown successfully")

	if err := a.redis.Close(); err != nil {
		log.Printf("redis close error: %v", err)
	}
	log.Println("redis connection closed")

	if err := a.mysql.Close(); err != nil {
		log.Printf("mysql close error: %v", err)
	}
	log.Println("mysql connection closed")

	return nil
}
