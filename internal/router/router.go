package router

import (
	"cw3/internal/controller"
	"cw3/internal/middleware"

	"github.com/gin-gonic/gin"
)

func SetupRouter(streamController *controller.StreamController) *gin.Engine {
	r := gin.Default()

	r.Use(middleware.CORSMiddleware())
	r.Use(middleware.RequestLogMiddleware())
	r.Use(middleware.RecoveryMiddleware())

	apiV1 := r.Group("/api/v1")
	{
		stream := apiV1.Group("/stream")
		{
			stream.POST("/report", middleware.AuthMiddleware(), streamController.ReportStreamQuality)

			stream.POST("/control", middleware.OperatorAuthMiddleware(), streamController.ControlStream)

			stream.POST("/cdn/switch", middleware.OperatorAuthMiddleware(), streamController.BatchSwitchCDN)

			stream.GET("/info/:room_id", streamController.GetStreamInfo)

			stream.GET("/quality/:room_id/logs", streamController.GetQualityLogs)

			stream.GET("/control/logs", streamController.GetControlLogs)

			stream.GET("/cdn/logs", streamController.GetCDNSwitchLogs)

			stream.GET("/active", streamController.GetAllActiveStreams)
		}

		health := apiV1.Group("/health")
		{
			health.GET("/ping", func(c *gin.Context) {
				c.JSON(200, gin.H{
					"code":    0,
					"message": "pong",
				})
			})
		}
	}

	return r
}
