package web

import (
    "fmt"
    "log"
    "time"

    "teamdrive-scanner/database"

    "github.com/gofiber/fiber/v2"
    "github.com/gofiber/fiber/v2/middleware/compress"
    "github.com/gofiber/fiber/v2/middleware/cors"
    "github.com/gofiber/fiber/v2/middleware/limiter"
    "github.com/gofiber/fiber/v2/middleware/logger"
)

type Server struct {
    app        *fiber.App
    db         *database.Database
    teamDrives interface{}
}

func NewServer(db *database.Database, teamDrives interface{}) *Server {
    app := fiber.New(fiber.Config{
        Prefork:               true,
        CaseSensitive:         false,
        StrictRouting:         false,
        ServerHeader:          "TeamDrive Scanner",
        AppName:               "TeamDrive Scanner v1.0",
        ReadTimeout:           10 * time.Second,
        WriteTimeout:          10 * time.Second,
        IdleTimeout:           120 * time.Second,
        BodyLimit:             4 * 1024 * 1024,
        DisableStartupMessage: false,
    })

    app.Use(logger.New(logger.Config{
        Format:     "[${time}] ${status} - ${latency} ${method} ${path}
",
        TimeFormat: "2006-01-02 15:04:05",
    }))

    app.Use(cors.New(cors.Config{
        AllowOrigins: "*",
        AllowMethods: "GET,POST,HEAD,OPTIONS",
    }))

    app.Use(compress.New(compress.Config{
        Level: compress.LevelBestSpeed,
    }))

    app.Use(limiter.New(limiter.Config{
        Max:               100,
        Expiration:        1 * time.Minute,
        LimiterMiddleware: limiter.SlidingWindow{},
        KeyGenerator: func(c *fiber.Ctx) string {
            return c.IP()
        },
        LimitReached: func(c *fiber.Ctx) error {
            return c.Status(429).JSON(fiber.Map{
                "error": "Rate limit exceeded. Please try again later.",
            })
        },
    }))

    server := &Server{
        app:        app,
        db:         db,
        teamDrives: teamDrives,
    }

    server.setupRoutes()

    return server
}

func (s *Server) setupRoutes() {
    s.app.Get("/", func(c *fiber.Ctx) error {
        return c.SendFile("./static/index.html")
    })
    s.app.Static("/static", "./static")

    api := s.app.Group("/api")

    api.Get("/teamdrives", s.getTeamDrives)
    api.Get("/search", s.search)
    api.Get("/stats/:teamdrive_id", s.getStats)
}

func (s *Server) getTeamDrives(c *fiber.Ctx) error {
    return c.JSON(s.teamDrives)
}

func (s *Server) search(c *fiber.Ctx) error {
    query := c.Query("q", "")
    teamDriveID := c.Query("teamdrive", "")
    parentID := c.Query("parent", "")
    limit := c.QueryInt("limit", 100)
    offset := c.QueryInt("offset", 0)

    result, err := s.db.Search(query, teamDriveID, parentID, limit, offset)
    if err != nil {
        return c.Status(500).JSON(fiber.Map{"error": err.Error()})
    }

    return c.JSON(result)
}

func (s *Server) getStats(c *fiber.Ctx) error {
    teamDriveID := c.Params("teamdrive_id")

    stats := s.db.GetTeamDriveStats(teamDriveID)

    return c.JSON(stats)
}

func (s *Server) Start(host string, port int) error {
    addr := fmt.Sprintf("%s:%d", host, port)
    log.Printf("Server running at http://%s", addr)
    return s.app.Listen(addr)
}
