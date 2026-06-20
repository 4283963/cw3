package service

import (
	"context"
	"cw3/internal/model"
)

type StreamService interface {
	ReportStreamQuality(ctx context.Context, req *model.StreamReportRequest) (*model.StreamReportResponse, error)
	ControlStream(ctx context.Context, req *model.StreamControlRequest) (*model.StreamControlResponse, error)
	BatchSwitchCDN(ctx context.Context, req *model.BatchSwitchCDNRequest) (*model.BatchSwitchCDNResponse, error)
	GetStreamInfo(ctx context.Context, roomID string) (*model.StreamRealTimeInfo, error)
	GetStreamQualityLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamQualityLog, int64, error)
	GetControlLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamControlLog, int64, error)
	GetCDNSwitchLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.CDNSwitchLog, int64, error)
	GetAllActiveStreams(ctx context.Context) ([]string, error)
}
