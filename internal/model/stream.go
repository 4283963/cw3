package model

import (
	"time"

	"gorm.io/gorm"
)

type StreamStatus string

const (
	StreamStatusLive         StreamStatus = "live"
	StreamStatusLagging      StreamStatus = "lagging"
	StreamStatusDisconnected StreamStatus = "disconnected"
	StreamStatusStopped      StreamStatus = "stopped"
)

type CDNLineType string

const (
	CDNLinePrimary CDNLineType = "primary"
	CDNLineBackup  CDNLineType = "backup"
)

func (c CDNLineType) String() string {
	return string(c)
}

func (c CDNLineType) Valid() bool {
	return c == CDNLinePrimary || c == CDNLineBackup
}

type StreamSession struct {
	gorm.Model
	RoomID      string       `gorm:"column:room_id;size:64;not null;index:idx_room_status,priority:1" json:"room_id"`
	TeacherID   uint64       `gorm:"column:teacher_id;not null;index" json:"teacher_id"`
	CourseID    uint64       `gorm:"column:course_id;not null;index" json:"course_id"`
	Status      StreamStatus `gorm:"column:status;size:20;not null;index:idx_room_status,priority:2" json:"status"`
	StartTime   time.Time    `gorm:"column:start_time;not null" json:"start_time"`
	EndTime     *time.Time   `gorm:"column:end_time" json:"end_time,omitempty"`
	OnlineCount int          `gorm:"column:online_count;default:0" json:"online_count"`
	StopReason  string       `gorm:"column:stop_reason;size:255" json:"stop_reason,omitempty"`

	ActiveSessionLock string `gorm:"-" json:"-"`
}

func (StreamSession) TableName() string {
	return "stream_sessions"
}

type StreamQualityLog struct {
	gorm.Model
	RoomID       string       `gorm:"column:room_id;size:64;not null;index" json:"room_id"`
	TeacherID    uint64       `gorm:"column:teacher_id;not null;index" json:"teacher_id"`
	PacketLoss   float64      `gorm:"column:packet_loss;type:decimal(5,2);not null" json:"packet_loss"`
	FPS          int          `gorm:"column:fps;not null" json:"fps"`
	Bitrate      int          `gorm:"column:bitrate;not null" json:"bitrate"`
	Resolution   string       `gorm:"column:resolution;size:20" json:"resolution"`
	ReportedAt   time.Time    `gorm:"column:reported_at;not null;index" json:"reported_at"`
	StreamStatus StreamStatus `gorm:"column:stream_status;size:20;not null" json:"stream_status"`
}

func (StreamQualityLog) TableName() string {
	return "stream_quality_logs"
}

type StreamControlLog struct {
	gorm.Model
	RoomID     string    `gorm:"column:room_id;size:64;not null;index" json:"room_id"`
	OperatorID uint64    `gorm:"column:operator_id;not null" json:"operator_id"`
	Action     string    `gorm:"column:action;size:50;not null" json:"action"`
	Reason     string    `gorm:"column:reason;size:500" json:"reason"`
	OperatedAt time.Time `gorm:"column:operated_at;not null" json:"operated_at"`
}

func (StreamControlLog) TableName() string {
	return "stream_control_logs"
}

type CDNSwitchLog struct {
	gorm.Model
	RoomID     string      `gorm:"column:room_id;size:64;not null;index" json:"room_id"`
	OperatorID uint64      `gorm:"column:operator_id;not null;index" json:"operator_id"`
	FromLine   CDNLineType `gorm:"column:from_line;size:20;not null" json:"from_line"`
	ToLine     CDNLineType `gorm:"column:to_line;size:20;not null" json:"to_line"`
	FromURL    string      `gorm:"column:from_url;size:512" json:"from_url"`
	ToURL      string      `gorm:"column:to_url;size:512;not null" json:"to_url"`
	BatchID    string      `gorm:"column:batch_id;size:64;not null;index" json:"batch_id"`
	Reason     string      `gorm:"column:reason;size:500" json:"reason"`
	SwitchedAt time.Time   `gorm:"column:switched_at;not null;index" json:"switched_at"`
}

func (CDNSwitchLog) TableName() string {
	return "stream_cdn_switch_logs"
}
