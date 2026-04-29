package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"

	_ "github.com/mattn/go-sqlite3"
	qrterminal "github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type Config struct {
	CommandPrefix      string
	WAStorePath        string
	StatsPath          string
	HistoryBatchSize   int
	HistoryMaxRequests int
}

func main() {
	cfg := loadConfig()
	log := waLog.Stdout("Sachet", "INFO", true)

	ctx := context.Background()
	container, err := sqlstore.New(ctx, "sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", filepath.ToSlash(cfg.WAStorePath)), log)
	if err != nil {
		panic(fmt.Errorf("failed to open whatsmeow store: %w", err))
	}

	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		panic(fmt.Errorf("failed to load device from store: %w", err))
	}

	client := whatsmeow.NewClient(deviceStore, log.Sub("Client"))
	statsStore, err := NewStatsStore(cfg.StatsPath)
	if err != nil {
		panic(fmt.Errorf("failed to initialize stats store: %w", err))
	}

	bot := NewSachetBot(client, statsStore, cfg, log.Sub("Bot"))
	client.AddEventHandler(bot.HandleEvent)

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(ctx)
		if err != nil {
			panic(fmt.Errorf("failed to get QR channel: %w", err))
		}
		if err := client.Connect(); err != nil {
			panic(fmt.Errorf("failed to connect client: %w", err))
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("Scan this QR in WhatsApp:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				continue
			}
			fmt.Printf("QR channel event: %s\n", evt.Event)
		}
	} else {
		if err := client.Connect(); err != nil {
			panic(fmt.Errorf("failed to connect client: %w", err))
		}
	}

	fmt.Printf("Sachet is running with %d CPUs available\n", runtime.NumCPU())
	waitForShutdown(client, log)
}

func waitForShutdown(client *whatsmeow.Client, log waLog.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Infof("Shutting down")
	client.Disconnect()
}

func loadConfig() Config {
	return Config{
		CommandPrefix:      getEnv("SACHET_COMMAND_PREFIX", "."),
		WAStorePath:        getEnv("SACHET_WA_DB", "wa-session.db"),
		StatsPath:          getEnv("SACHET_STATS_FILE", "sachet-stats.json"),
		HistoryBatchSize:   getEnvInt("SACHET_HISTORY_BATCH", 50),
		HistoryMaxRequests: getEnvInt("SACHET_HISTORY_MAX_REQUESTS", 40),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return fallback
	}
	return val
}
