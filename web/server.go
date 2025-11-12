package web

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"teamdrive-scanner/database"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
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

	app.Use(recover.New())
	app.Use(logger.New(logger.Config{
		Format:     "[${time}] ${status} - ${latency} ${method} ${path}\n",
		TimeFormat: "2006-01-02 15:04:05",
	}))
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,HEAD,OPTIONS",
	}))
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
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

	s.app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	api := s.app.Group("/api")
	api.Get("/teamdrives", s.getTeamDrives)
	api.Get("/search", s.search)
	api.Get("/stats/:teamdrive_id", s.getStats)

	s.app.Use(func(c *fiber.Ctx) error {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Route not found",
		})
	})
}

// Handler: Get team drives list
func (s *Server) getTeamDrives(c *fiber.Ctx) error {
	return c.JSON(s.teamDrives)
}

// Handler: Search files
func (s *Server) search(c *fiber.Ctx) error {
	query := c.Query("q", "")
	teamDriveID := c.Query("teamdrive", "")
	parentID := c.Query("parent", "")

	limit, err := strconv.Atoi(c.Query("limit", "100"))
	if err != nil || limit <= 0 || limit > 1000 {
		limit = 100
	}

	offset, err := strconv.Atoi(c.Query("offset", "0"))
	if err != nil || offset < 0 {
		offset = 0
	}

	result, err := s.db.Search(query, teamDriveID, parentID, limit, offset)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error": "Search failed: " + err.Error(),
		})
	}

	return c.JSON(result)
}

// Handler: Get team drive statistics
func (s *Server) getStats(c *fiber.Ctx) error {
	teamDriveID := c.Params("teamdrive_id")

	if teamDriveID == "" {
		return c.Status(400).JSON(fiber.Map{
			"error": "Team Drive ID is required",
		})
	}

	stats := s.db.GetTeamDriveStats(teamDriveID)
	return c.JSON(stats)
}

// Start server
func (s *Server) Start(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("🚀 Server starting on http://%s", addr)
	log.Printf("📁 Access the web interface at http://%s", addr)
	return s.app.Listen(addr)
}
