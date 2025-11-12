package scanner

import (
    "context"
    "fmt"
    "io/ioutil"
    "log"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"

    "teamdrive-scanner/database"

    "golang.org/x/time/rate"
    "google.golang.org/api/drive/v3"
    "google.golang.org/api/googleapi"
    "google.golang.org/api/option"
)

type ServiceAccountPool struct {
    services []*drive.Service
    limiters []*rate.Limiter
    current  atomic.Int32
}

type ScanConfig struct {
    TeamDriveID       string
    TeamDriveName     string
    WorkersPerAccount int
    PageSize          int64
    BatchInsertSize   int
}

type Stats struct {
    TeamDriveName   string
    FilesProcessed  atomic.Int64
    FoldersQueued   atomic.Int64
    APICallsTotal   atomic.Int64
    APICallsSuccess atomic.Int64
    APICallsFailed  atomic.Int64
    DBInserts       atomic.Int64
    StartTime       time.Time
}

type Worker struct {
    id          int
    pool        *ServiceAccountPool
    jobQueue    <-chan string
    resultQueue chan<- database.FileRecord
    wg          *sync.WaitGroup
    ctx         context.Context
    stats       *Stats
    config      ScanConfig
}

func InitServiceAccountPool(saDir string, ratePerAccount int) (*ServiceAccountPool, error) {
    files, err := ioutil.ReadDir(saDir)
    if err != nil {
        return nil, fmt.Errorf("cannot read service accounts directory: %w", err)
    }

    pool := &ServiceAccountPool{
        services: make([]*drive.Service, 0),
        limiters: make([]*rate.Limiter, 0),
    }

    ctx := context.Background()

    for _, file := range files {
        if filepath.Ext(file.Name()) != ".json" {
            continue
        }

        credPath := filepath.Join(saDir, file.Name())
        credentials, err := ioutil.ReadFile(credPath)
        if err != nil {
            log.Printf("Skipping %s: %v", file.Name(), err)
            continue
        }

        service, err := drive.NewService(ctx,
            option.WithCredentialsJSON(credentials),
            option.WithScopes(drive.DriveReadonlyScope),
        )
        if err != nil {
            log.Printf("Skipping %s: %v", file.Name(), err)
            continue
        }

        pool.services = append(pool.services, service)
        pool.limiters = append(pool.limiters,
            rate.NewLimiter(rate.Limit(ratePerAccount), ratePerAccount*2))
    }

    if len(pool.services) == 0 {
        return nil, fmt.Errorf("no valid service accounts found in %s", saDir)
    }

    return pool, nil
}

func (p *ServiceAccountPool) getNext() (*drive.Service, *rate.Limiter) {
    idx := int(p.current.Add(1)) % len(p.services)
    return p.services[idx], p.limiters[idx]
}

func (p *ServiceAccountPool) Count() int {
    return len(p.services)
}

func ScanTeamDrive(config ScanConfig, db *database.Database, pool *ServiceAccountPool) error {
    ctx := context.Background()
    stats := &Stats{
        TeamDriveName: config.TeamDriveName,
        StartTime:     time.Now(),
    }

    totalWorkers := pool.Count() * config.WorkersPerAccount
    log.Printf("[%s] Starting with %d workers (%d SAs × %d workers/SA)",
        config.TeamDriveName, totalWorkers, pool.Count(), config.WorkersPerAccount)

    jobQueue := make(chan string, totalWorkers*10)
    resultQueue := make(chan database.FileRecord, 100000)

    dbDone := make(chan struct{})
    go dbWriter(db, resultQueue, dbDone, stats, config.BatchInsertSize)

    var wg sync.WaitGroup
    for i := 0; i < totalWorkers; i++ {
        wg.Add(1)
        worker := Worker{
            id:          i,
            pool:        pool,
            jobQueue:    jobQueue,
            resultQueue: resultQueue,
            wg:          &wg,
            ctx:         ctx,
            stats:       stats,
            config:      config,
        }
        go worker.start()
    }

    stopStats := make(chan struct{})
    go logStats(stats, stopStats)

    jobQueue <- config.TeamDriveID

    wg.Wait()
    close(resultQueue)
    <-dbDone
    close(stopStats)

    printFinalStats(stats, pool.Count())

    return nil
}

func (w *Worker) start() {
    defer w.wg.Done()

    for folderID := range w.jobQueue {
        if err := w.listFolder(folderID); err != nil {
            log.Printf("[%s] Worker-%d: Error listing %s: %v",
                w.config.TeamDriveName, w.id, folderID, err)
            w.stats.APICallsFailed.Add(1)
        }
    }
}

func (w *Worker) listFolder(folderID string) error {
    service, limiter := w.pool.getNext()
    pageToken := ""

    for {
        if err := limiter.Wait(w.ctx); err != nil {
            return err
        }

        query := fmt.Sprintf("'%s' in parents and trashed=false", folderID)

        w.stats.APICallsTotal.Add(1)

        call := service.Files.List().
            Q(query).
            PageSize(w.config.PageSize).
            SupportsAllDrives(true).
            IncludeItemsFromAllDrives(true).
            Corpora("drive").
            DriveId(w.config.TeamDriveID).
            Fields("nextPageToken, files(id, name, size, modifiedTime, mimeType)").
            PageToken(pageToken)

        fileList, err := w.executeWithRetry(call, limiter)
        if err != nil {
            return err
        }

        w.stats.APICallsSuccess.Add(1)

        for _, file := range fileList.Files {
            isFolder := file.MimeType == "application/vnd.google-apps.folder"

            record := database.FileRecord{
                ID:            file.Id,
                Name:          file.Name,
                ParentID:      folderID,
                TeamDriveID:   w.config.TeamDriveID,
                TeamDriveName: w.config.TeamDriveName,
                Size:          file.Size,
                ModifiedTime:  file.ModifiedTime,
                MimeType:      file.MimeType,
                IsFolder:      isFolder,
                Path:          file.Name,
            }

            w.resultQueue <- record
            w.stats.FilesProcessed.Add(1)

            if isFolder {
                w.stats.FoldersQueued.Add(1)
                w.wg.Add(1)
                go func(id string) {
                    w.jobQueue <- id
                }(file.Id)
            }
        }

        pageToken = fileList.NextPageToken
        if pageToken == "" {
            break
        }
    }

    return nil
}

func (w *Worker) executeWithRetry(call *drive.FilesListCall, limiter *rate.Limiter) (*drive.FileList, error) {
    maxRetries := 5
    baseDelay := time.Second

    for attempt := 0; attempt < maxRetries; attempt++ {
        fileList, err := call.Do()
        if err == nil {
            return fileList, nil
        }

        if gerr, ok := err.(*googleapi.Error); ok {
            if gerr.Code == 403 || gerr.Code == 429 {
                delay := baseDelay * time.Duration(1<<uint(attempt))
                log.Printf("[%s] Worker-%d: Rate limit, waiting %v",
                    w.config.TeamDriveName, w.id, delay)
                time.Sleep(delay)
                continue
            }
        }

        if attempt < maxRetries-1 {
            delay := baseDelay * time.Duration(1<<uint(attempt))
            time.Sleep(delay)
            continue
        }

        return nil, err
    }

    return nil, fmt.Errorf("max retries exceeded")
}

func dbWriter(db *database.Database, resultQueue <-chan database.FileRecord, done chan<- struct{}, stats *Stats, batchSize int) {
    defer close(done)

    batch := make([]database.FileRecord, 0, batchSize)
    ticker := time.NewTicker(2 * time.Second)
    defer ticker.Stop()

    flush := func() {
        if len(batch) == 0 {
            return
        }

        if err := db.BatchInsert(batch); err != nil {
            log.Printf("[%s] DB insert failed: %v", stats.TeamDriveName, err)
        } else {
            stats.DBInserts.Add(int64(len(batch)))
        }

        batch = batch[:0]
    }

    for {
        select {
        case record, ok := <-resultQueue:
            if !ok {
                flush()
                return
            }

            batch = append(batch, record)

            if len(batch) >= batchSize {
                flush()
            }

        case <-ticker.C:
            flush()
        }
    }
}

func logStats(stats *Stats, stop <-chan struct{}) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            printStats(stats, 0)
        case <-stop:
            return
        }
    }
}

func printStats(stats *Stats, accountCount int) {
    elapsed := time.Since(stats.StartTime)
    files := stats.FilesProcessed.Load()
    apiCalls := stats.APICallsTotal.Load()
    apiSuccess := stats.APICallsSuccess.Load()
    apiFailed := stats.APICallsFailed.Load()
    dbInserts := stats.DBInserts.Load()
    folders := stats.FoldersQueued.Load()

    filesPerSec := float64(files) / elapsed.Seconds()
    apiPerSec := float64(apiCalls) / elapsed.Seconds()
    successRate := 0.0
    if apiCalls > 0 {
        successRate = float64(apiSuccess) / float64(apiCalls) * 100
    }

    log.Printf("
==== [%s] STATS ====", stats.TeamDriveName)
    log.Printf("Elapsed:        %v", elapsed.Round(time.Second))
    log.Printf("Files:          %d (%.0f/sec)", files, filesPerSec)
    log.Printf("Folders:        %d", folders)
    log.Printf("API Calls:      %d (%.1f/sec)", apiCalls, apiPerSec)
    log.Printf("API Success:    %d (%.1f%%)", apiSuccess, successRate)
    log.Printf("API Failed:     %d", apiFailed)
    log.Printf("DB Inserts:     %d", dbInserts)

    if accountCount > 0 {
        log.Printf("Accounts Used:  %d", accountCount)
    }

    log.Println("========================")
}

func printFinalStats(stats *Stats, accountCount int) {
    elapsed := time.Since(stats.StartTime)
    files := stats.FilesProcessed.Load()

    log.Printf("
=== [%s] FINAL STATS ===", stats.TeamDriveName)
    printStats(stats, accountCount)

    log.Printf("
[%s] Total Duration: %v", stats.TeamDriveName, elapsed.Round(time.Millisecond))
    log.Printf("[%s] Average Rate: %.0f files/sec", stats.TeamDriveName, float64(files)/elapsed.Seconds())
    log.Printf("[%s] Service Accounts: %d", stats.TeamDriveName, accountCount)
    log.Println("==============================")
}
