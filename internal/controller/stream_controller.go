package controller

import (
	"net/http"
	"strconv"

	"cw3/internal/model"
	"cw3/internal/service"

	"github.com/gin-gonic/gin"
)

type StreamController struct {
	streamService service.StreamService
}

func NewStreamController(streamService service.StreamService) *StreamController {
	return &StreamController{
		streamService: streamService,
	}
}

func (c *StreamController) ReportStreamQuality(ctx *gin.Context) {
	var req model.StreamReportRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request body: " + err.Error(),
		})
		return
	}

	resp, err := c.streamService.ReportStreamQuality(ctx.Request.Context(), &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "report stream quality failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

func (c *StreamController) ControlStream(ctx *gin.Context) {
	var req model.StreamControlRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request body: " + err.Error(),
		})
		return
	}

	resp, err := c.streamService.ControlStream(ctx.Request.Context(), &req)
	if err != nil {
		if err.Error() == "stream room "+req.RoomID+" not found or not active" {
			ctx.JSON(http.StatusNotFound, model.Response{
				Code:    404,
				Message: err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "control stream failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

func (c *StreamController) GetStreamInfo(ctx *gin.Context) {
	roomID := ctx.Param("room_id")
	if roomID == "" {
		ctx.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "room_id is required",
		})
		return
	}

	info, err := c.streamService.GetStreamInfo(ctx.Request.Context(), roomID)
	if err != nil {
		if err.Error() == "stream room "+roomID+" not found" {
			ctx.JSON(http.StatusNotFound, model.Response{
				Code:    404,
				Message: err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "get stream info failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data:    info,
	})
}

func (c *StreamController) GetQualityLogs(ctx *gin.Context) {
	roomID := ctx.Param("room_id")
	if roomID == "" {
		ctx.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "room_id is required",
		})
		return
	}

	page, _ := strconv.Atoi(ctx.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(ctx.DefaultQuery("page_size", "20"))

	logs, total, err := c.streamService.GetStreamQualityLogs(ctx.Request.Context(), roomID, page, pageSize)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "get quality logs failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"list":  logs,
			"total": total,
			"page":  page,
			"size":  pageSize,
		},
	})
}

func (c *StreamController) GetControlLogs(ctx *gin.Context) {
	roomID := ctx.Query("room_id")

	page, _ := strconv.Atoi(ctx.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(ctx.DefaultQuery("page_size", "20"))

	logs, total, err := c.streamService.GetControlLogs(ctx.Request.Context(), roomID, page, pageSize)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "get control logs failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"list":  logs,
			"total": total,
			"page":  page,
			"size":  pageSize,
		},
	})
}

func (c *StreamController) GetAllActiveStreams(ctx *gin.Context) {
	roomIDs, err := c.streamService.GetAllActiveStreams(ctx.Request.Context())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "get all active streams failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"rooms": roomIDs,
			"count": len(roomIDs),
		},
	})
}

func (c *StreamController) BatchSwitchCDN(ctx *gin.Context) {
	var req model.BatchSwitchCDNRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, model.Response{
			Code:    400,
			Message: "invalid request body: " + err.Error(),
		})
		return
	}

	if req.TargetLine == string(model.CDNLineBackup) && req.BackupURL == "" {
		req.BackupURL = model.DefaultBackupCDNURL
	}

	resp, err := c.streamService.BatchSwitchCDN(ctx.Request.Context(), &req)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "batch switch cdn failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, resp)
}

func (c *StreamController) GetCDNSwitchLogs(ctx *gin.Context) {
	roomID := ctx.Query("room_id")

	page, _ := strconv.Atoi(ctx.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(ctx.DefaultQuery("page_size", "20"))

	logs, total, err := c.streamService.GetCDNSwitchLogs(ctx.Request.Context(), roomID, page, pageSize)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, model.Response{
			Code:    500,
			Message: "get cdn switch logs failed: " + err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, model.Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"list":  logs,
			"total": total,
			"page":  page,
			"size":  pageSize,
		},
	})
}
