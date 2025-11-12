package database

import (
    "database/sql"
    "fmt"
    "log"
    "sync"
    "time"

    _ "github.com/mattn/go-sqlite3"
)

type Database struct {
    db    *sql.DB
    mutex sync.Mutex
}

type FileRecord struct {
    ID            string `json:"id"`
    Name          string `json:"name"`
    ParentID      string `json:"parent_id"`
    TeamDriveID   string `json:"teamdrive_id"`
    TeamDriveName string `json:"teamdrive_name"`
    Size          int64  `json:"size"`
    ModifiedTime  string `json:"modified_time"`
    MimeType      string `json:"mime_type"`
    IsFolder      bool   `json:"is_folder"`
    Path          string `json:"path"`
    TotalSize     int64  `json:"total_size"`
    ChildCount    int    `json:"child_count"`
}

type SearchResult struct {
    Files      []FileRecord `json:"files"`
    TotalCount int          `json:"total_count"`
}


func InitDatabase(path string, cacheSizeMB int) (*Database, error) {
    db, err := sql.Open("sqlite3", fmt.Sprintf("%s?cache=shared&mode=rwc&_journal_mode=WAL&_busy_timeout=5000", path))
    if err != nil {
        return nil, err
    }

    pragmas := []string{
        "PRAGMA synchronous = NORMAL",
        fmt.Sprintf("PRAGMA cache_size = -%d", cacheSizeMB*1024),
        "PRAGMA temp_store = MEMORY",
        "PRAGMA mmap_size = 30000000000",
        "PRAGMA page_size = 4096",
        "PRAGMA journal_mode = WAL",
        "PRAGMA wal_autocheckpoint = 1000",
        "PRAGMA busy_timeout = 5000",
    }

    for _, pragma := range pragmas {
        if _, err := db.Exec(pragma); err != nil {
            return nil, fmt.Errorf("pragma failed: %w", err)
        }
    }

    db.SetMaxOpenConns(100)
    db.SetMaxIdleConns(10)
    db.SetConnMaxLifetime(time.Hour)

    schema := `
    CREATE TABLE IF NOT EXISTS files (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL COLLATE NOCASE,
        parent_id TEXT,
        teamdrive_id TEXT NOT NULL,
        teamdrive_name TEXT NOT NULL,
        size INTEGER DEFAULT 0,
        modified_time TEXT,
        mime_type TEXT,
        is_folder BOOLEAN,
        path TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) WITHOUT ROWID;

    CREATE INDEX IF NOT EXISTS idx_parent ON files(parent_id);
    CREATE INDEX IF NOT EXISTS idx_teamdrive ON files(teamdrive_id);
    CREATE INDEX IF NOT EXISTS idx_name_lower ON files(name COLLATE NOCASE);
    CREATE INDEX IF NOT EXISTS idx_size ON files(size DESC);
    CREATE INDEX IF NOT EXISTS idx_modified ON files(modified_time DESC);
    CREATE INDEX IF NOT EXISTS idx_folder ON files(is_folder, parent_id);
    `

    if _, err := db.Exec(schema); err != nil {
        return nil, fmt.Errorf("schema creation failed: %w", err)
    }

    // Simplified FTS5 for maximum compatibility
    ftsSchema := `
    CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
        id UNINDEXED,
        name,
        path,
        teamdrive_name UNINDEXED,
        content='files',
        content_rowid='rowid'
    );

    CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
        INSERT INTO files_fts(rowid, id, name, path, teamdrive_name)
        VALUES (new.rowid, new.id, new.name, new.path, new.teamdrive_name);
    END;

    CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
        INSERT INTO files_fts(files_fts, rowid, id, name, path, teamdrive_name)
        VALUES('delete', old.rowid, old.id, old.name, old.path, old.teamdrive_name);
    END;

    CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
        INSERT INTO files_fts(files_fts, rowid, id, name, path, teamdrive_name)
        VALUES('delete', old.rowid, old.id, old.name, old.path, old.teamdrive_name);
        INSERT INTO files_fts(rowid, id, name, path, teamdrive_name)
        VALUES (new.rowid, new.id, new.name, new.path, new.teamdrive_name);
    END;
    `

    if _, err := db.Exec(ftsSchema); err != nil {
        return nil, fmt.Errorf("FTS5 setup failed: %w", err)
    }

    log.Println("Database initialized: SQLite with WAL mode + FTS5")
    log.Printf("Configuration: %dMB cache, 100 max connections", cacheSizeMB)

    return &Database{db: db}, nil
}

func (d *Database) BatchInsert(records []FileRecord) error {
    d.mutex.Lock()
    defer d.mutex.Unlock()

    start := time.Now()

    tx, err := d.db.Begin()
    if err != nil {
        return err
    }

    stmt, err := tx.Prepare(`
        INSERT OR REPLACE INTO files 
        (id, name, parent_id, teamdrive_id, teamdrive_name, size, modified_time, mime_type, is_folder, path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
    if err != nil {
        tx.Rollback()
        return err
    }
    defer stmt.Close()

    for _, record := range records {
        _, err := stmt.Exec(
            record.ID,
            record.Name,
            record.ParentID,
            record.TeamDriveID,
            record.TeamDriveName,
            record.Size,
            record.ModifiedTime,
            record.MimeType,
            record.IsFolder,
            record.Path,
        )
        if err != nil {
            log.Printf("Insert failed for %s: %v", record.Name, err)
        }
    }

    if err := tx.Commit(); err != nil {
        return err
    }

    duration := time.Since(start)
    rate := float64(len(records)) / duration.Seconds()
    log.Printf("DB: Inserted %d records in %v (%.0f/sec)", len(records), duration.Round(time.Millisecond), rate)

    return nil
}

func (d *Database) Search(query string, teamDriveID string, parentID string, limit int, offset int) (*SearchResult, error) {
    var records []FileRecord
    var totalCount int

    if query != "" {
        searchQuery := `
            SELECT f.id, f.name, f.parent_id, f.teamdrive_id, f.teamdrive_name, 
                   f.size, f.modified_time, f.mime_type, f.is_folder, f.path
            FROM files_fts fts
            JOIN files f ON fts.rowid = f.rowid
            WHERE files_fts MATCH ?
        `
        args := []interface{}{query}

        if teamDriveID != "" {
            searchQuery += " AND f.teamdrive_id = ?"
            args = append(args, teamDriveID)
        }
        if parentID != "" {
            searchQuery += " AND f.parent_id = ?"
            args = append(args, parentID)
        }

        searchQuery += " ORDER BY rank LIMIT ? OFFSET ?"
        args = append(args, limit, offset)

        rows, err := d.db.Query(searchQuery, args...)
        if err != nil {
            return nil, err
        }
        defer rows.Close()

        records = d.scanRows(rows)

        countQuery := "SELECT COUNT(*) FROM files_fts WHERE files_fts MATCH ?"
        countArgs := []interface{}{query}
        if teamDriveID != "" {
            countQuery = "SELECT COUNT(*) FROM files_fts fts JOIN files f ON fts.rowid = f.rowid WHERE files_fts MATCH ? AND f.teamdrive_id = ?"
            countArgs = append(countArgs, teamDriveID)
        }
        d.db.QueryRow(countQuery, countArgs...).Scan(&totalCount)

    } else {
        listQuery := `
            SELECT id, name, parent_id, teamdrive_id, teamdrive_name, 
                   size, modified_time, mime_type, is_folder, path
            FROM files
            WHERE 1=1
        `
        args := []interface{}{}

        if teamDriveID != "" {
            listQuery += " AND teamdrive_id = ?"
            args = append(args, teamDriveID)
        }
        if parentID != "" {
            listQuery += " AND parent_id = ?"
            args = append(args, parentID)
        } else if teamDriveID != "" {
            listQuery += " AND parent_id = teamdrive_id"
        }

        listQuery += " ORDER BY is_folder DESC, name ASC LIMIT ? OFFSET ?"
        args = append(args, limit, offset)

        rows, err := d.db.Query(listQuery, args...)
        if err != nil {
            return nil, err
        }
        defer rows.Close()

        records = d.scanRows(rows)

        countQuery := "SELECT COUNT(*) FROM files WHERE 1=1"
        countArgs := []interface{}{}
        if teamDriveID != "" {
            countQuery += " AND teamdrive_id = ?"
            countArgs = append(countArgs, teamDriveID)
        }
        if parentID != "" {
            countQuery += " AND parent_id = ?"
            countArgs = append(countArgs, parentID)
        } else if teamDriveID != "" {
            countQuery += " AND parent_id = teamdrive_id"
        }
        d.db.QueryRow(countQuery, countArgs...).Scan(&totalCount)
    }

    for i := range records {
        if records[i].IsFolder {
            records[i].TotalSize, records[i].ChildCount = d.GetFolderSize(records[i].ID)
        } else {
            records[i].TotalSize = records[i].Size
        }
    }

    return &SearchResult{
        Files:      records,
        TotalCount: totalCount,
    }, nil
}

func (d *Database) scanRows(rows *sql.Rows) []FileRecord {
    var records []FileRecord

    for rows.Next() {
        var record FileRecord
        var parentID, path sql.NullString

        err := rows.Scan(
            &record.ID,
            &record.Name,
            &parentID,
            &record.TeamDriveID,
            &record.TeamDriveName,
            &record.Size,
            &record.ModifiedTime,
            &record.MimeType,
            &record.IsFolder,
            &path,
        )

        if err != nil {
            log.Printf("Scan error: %v", err)
            continue
        }

        if parentID.Valid {
            record.ParentID = parentID.String
        }
        if path.Valid {
            record.Path = path.String
        }

        records = append(records, record)
    }

    return records
}

func (d *Database) GetFolderSize(folderID string) (int64, int) {
    var totalSize int64
    var childCount int

    query := `
        WITH RECURSIVE folder_tree AS (
            SELECT id, size, is_folder
            FROM files
            WHERE parent_id = ?

            UNION ALL

            SELECT f.id, f.size, f.is_folder
            FROM files f
            JOIN folder_tree ft ON f.parent_id = ft.id
        )
        SELECT COALESCE(SUM(size), 0), COUNT(*)
        FROM folder_tree
    `

    d.db.QueryRow(query, folderID).Scan(&totalSize, &childCount)

    return totalSize, childCount
}

func (d *Database) GetTeamDriveStats(teamDriveID string) map[string]interface{} {
    stats := make(map[string]interface{})

    var totalFiles, totalFolders int64
    var totalSize int64

    d.db.QueryRow(`
        SELECT COUNT(*), COALESCE(SUM(size), 0)
        FROM files
        WHERE teamdrive_id = ? AND is_folder = 0
    `, teamDriveID).Scan(&totalFiles, &totalSize)

    d.db.QueryRow(`
        SELECT COUNT(*)
        FROM files
        WHERE teamdrive_id = ? AND is_folder = 1
    `, teamDriveID).Scan(&totalFolders)

    stats["total_files"] = totalFiles
    stats["total_folders"] = totalFolders
    stats["total_size"] = totalSize
    stats["total_size_human"] = formatBytes(totalSize)

    return stats
}

func formatBytes(bytes int64) string {
    const unit = 1024
    if bytes < unit {
        return fmt.Sprintf("%d B", bytes)
    }
    div, exp := int64(unit), 0
    for n := bytes / unit; n >= unit; n /= unit {
        div *= unit
        exp++
    }
    return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (d *Database) Close() error {
    log.Println("Optimizing database...")
    d.db.Exec("PRAGMA optimize")
    d.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
    return d.db.Close()
}
