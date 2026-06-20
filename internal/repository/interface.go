package repository

import (
	"context"
	"cw3/internal/model"
)

type StreamRedisRepository interface {
	SetStreamInfo(ctx context.Context, info *model.StreamRealTimeInfo, expireSec int) error
	GetStreamInfo(ctx context.Context, roomID string) (*model.StreamRealTimeInfo, error)
	DeleteStreamInfo(ctx context.Context, roomID string) error

	SetOnlineCount(ctx context.Context, roomID string, count int, expireSec int) error
	GetOnlineCount(ctx context.Context, roomID string) (int, error)
	IncrOnlineCount(ctx context.Context, roomID string) (int64, error)
	DecrOnlineCount(ctx context.Context, roomID string) (int64, error)

	SetControlSignal(ctx context.Context, roomID string, action string, expireSec int) error
	GetControlSignal(ctx context.Context, roomID string) (string, error)
	DeleteControlSignal(ctx context.Context, roomID string) error

	SetStreamQuality(ctx context.Context, roomID string, report *model.StreamReportRequest, expireSec int) error
	GetStreamQuality(ctx context.Context, roomID string) (*model.StreamReportRequest, error)

	CheckStreamExists(ctx context.Context, roomID string) (bool, error)
	GetAllActiveStreams(ctx context.Context) ([]string, error)
}

type StreamMySQLRepository interface {
	CreateStreamSession(ctx context.Context, session *model.StreamSession) error
	UpdateStreamSession(ctx context.Context, session *model.StreamSession) error
	GetActiveSessionByRoomID(ctx context.Context, roomID string) (*model.StreamSession, error)
	StopStreamSession(ctx context.Context, roomID, reason string) error

	CreateQualityLog(ctx context.Context, log *model.StreamQualityLog) error
	BatchCreateQualityLogs(ctx context.Context, logs []*model.StreamQualityLog) error
	GetQualityLogsByRoomID(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamQualityLog, int64, error)

	CreateControlLog(ctx context.Context, log *model.StreamControlLog) error
	GetControlLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamControlLog, int64, error)
}
