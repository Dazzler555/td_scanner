package main

import (
    "encoding/json"
    "flag"
    "log"
    "os"
    "sync"

    "teamdrive-scanner/database"
    "teamdrive-scanner/scanner"
    "teamdrive-scanner/web"
)

type TeamDrive struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

type Config struct {
    ServiceAccountsDir string      `json:"service_accounts_dir"`
    TeamDrives         []TeamDrive `json:"teamdrives"`
    Scanner            struct {
        WorkersPerAccount    int `json:"workers_per_account"`
        RatePerAccount       int `json:"rate_per_account"`
        PageSize             int64 `json:"page_size"`
        BatchInsertSize      int `json:"batch_insert_size"`
        ConcurrentTeamDrives int `json:"concurrent_teamdrives"`
    } `json:"scanner"`
    Database struct {
        Path        string `json:"path"`
        CacheSizeMB int    `json:"cache_size_mb"`
    } `json:"database"`
    Web struct {
        Port int    `json:"port"`
        Host string `json:"host"`
    } `json:"web"`
}

func main() {
    configPath := flag.String("config", "config.json", "Path to config file")
    mode := flag.String("mode", "web", "Mode: scan or web")
    flag.Parse()

    config, err := loadConfig(*configPath)
    if err != nil {
        log.Fatalf("Failed to load config: %v", err)
    }

    db, err := database.InitDatabase(config.Database.Path, config.Database.CacheSizeMB)
    if err != nil {
        log.Fatalf("Failed to initialize database: %v", err)
    }
    defer db.Close()

    switch *mode {
    case "scan":
        runScan(config, db)
    case "web":
        runWeb(config, db)
    default:
        log.Fatalf("Invalid mode: %s. Use 'scan' or 'web'", *mode)
    }
}

func loadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }

    var config Config
    if err := json.Unmarshal(data, &config); err != nil {
        return nil, err
    }

    return &config, nil
}

func runScan(config *Config, db *database.Database) {
    log.Println("=== Starting Multi-TeamDrive Scan ===")
    log.Printf("Service Accounts: %s", config.ServiceAccountsDir)
    log.Printf("Team Drives: %d", len(config.TeamDrives))
    log.Printf("Concurrent Team Drives: %d", config.Scanner.ConcurrentTeamDrives)

    pool, err := scanner.InitServiceAccountPool(config.ServiceAccountsDir, config.Scanner.RatePerAccount)
    if err != nil {
        log.Fatalf("Failed to initialize service account pool: %v", err)
    }
    log.Printf("Loaded %d service accounts", pool.Count())

    var wg sync.WaitGroup
    semaphore := make(chan struct{}, config.Scanner.ConcurrentTeamDrives)

    for _, td := range config.TeamDrives {
        wg.Add(1)
        semaphore <- struct{}{}

        go func(td TeamDrive) {
            defer wg.Done()
            defer func() { <-semaphore }()

            log.Printf("Starting scan: %s", td.Name)

            scanConfig := scanner.ScanConfig{
                TeamDriveID:       td.ID,
                TeamDriveName:     td.Name,
                WorkersPerAccount: config.Scanner.WorkersPerAccount,
                PageSize:          config.Scanner.PageSize,
                BatchInsertSize:   config.Scanner.BatchInsertSize,
            }

            if err := scanner.ScanTeamDrive(scanConfig, db, pool); err != nil {
                log.Printf("Error scanning %s: %v", td.Name, err)
            } else {
                log.Printf("Completed scan: %s", td.Name)
            }
        }(td)
    }

    wg.Wait()
    log.Println("=== All Scans Complete ===")
}

func runWeb(config *Config, db *database.Database) {
    log.Printf("Starting web server on %s:%d", config.Web.Host, config.Web.Port)
    log.Printf("Access at: http://localhost:%d", config.Web.Port)

    server := web.NewServer(db, config.TeamDrives)
    if err := server.Start(config.Web.Host, config.Web.Port); err != nil {
        log.Fatalf("Server error: %v", err)
    }
}
