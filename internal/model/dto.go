package model

type StreamReportRequest struct {
	RoomID     string  `json:"room_id" binding:"required,max=64"`
	TeacherID  uint64  `json:"teacher_id" binding:"required,min=1"`
	PacketLoss float64 `json:"packet_loss" binding:"required,min=0,max=100"`
	FPS        int     `json:"fps" binding:"required,min=0"`
	Bitrate    int     `json:"bitrate" binding:"required,min=0"`
	Resolution string  `json:"resolution" binding:"max=20"`
}

type StreamReportResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Status      StreamStatus `json:"status"`
		OnlineCount int          `json:"online_count"`
		ReportedAt  int64        `json:"reported_at"`
	} `json:"data"`
}

type StreamControlRequest struct {
	RoomID     string `json:"room_id" binding:"required,max=64"`
	OperatorID uint64 `json:"operator_id" binding:"required,min=1"`
	Action     string `json:"action" binding:"required,oneof=stop restart"`
	Reason     string `json:"reason" binding:"required,max=500"`
}

type StreamControlResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		RoomID     string `json:"room_id"`
		Action     string `json:"action"`
		OperatedAt int64  `json:"operated_at"`
	} `json:"data"`
}

type StreamRealTimeInfo struct {
	RoomID       string       `json:"room_id"`
	TeacherID    uint64       `json:"teacher_id"`
	CourseID     uint64       `json:"course_id"`
	Status       StreamStatus `json:"status"`
	OnlineCount  int          `json:"online_count"`
	PacketLoss   float64      `json:"packet_loss"`
	FPS          int          `json:"fps"`
	Bitrate      int          `json:"bitrate"`
	LastReportAt int64        `json:"last_report_at"`
	CDNLine      CDNLineType  `json:"cdn_line"`
	CDNURL       string       `json:"cdn_url"`
}

type BatchSwitchCDNRequest struct {
	RoomIDs    []string `json:"room_ids" binding:"required,min=1,max=100,dive,required,max=64"`
	OperatorID uint64   `json:"operator_id" binding:"required,min=1"`
	TargetLine string   `json:"target_line" binding:"required,oneof=primary backup"`
	BackupURL  string   `json:"backup_url" binding:"required_if=TargetLine backup,url"`
	PrimaryURL string   `json:"primary_url" binding:"omitempty,url"`
	Reason     string   `json:"reason" binding:"required,max=500"`
}

type BatchSwitchCDNResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		SwitchedAt   int64                 `json:"switched_at"`
		TargetLine   CDNLineType           `json:"target_line"`
		Success      []CDNSwitchResultItem `json:"success"`
		Failed       []CDNSwitchResultItem `json:"failed"`
		SuccessCount int                   `json:"success_count"`
		FailedCount  int                   `json:"failed_count"`
	} `json:"data"`
}

type CDNSwitchResultItem struct {
	RoomID string `json:"room_id"`
	CDNURL string `json:"cdn_url,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
