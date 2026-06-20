package service

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cw3/internal/model"
	"cw3/internal/repository"
)

type streamService struct {
	redisRepo repository.StreamRedisRepository
	mysqlRepo repository.StreamMySQLRepository
}

func NewStreamService(redisRepo repository.StreamRedisRepository, mysqlRepo repository.StreamMySQLRepository) StreamService {
	return &streamService{
		redisRepo: redisRepo,
		mysqlRepo: mysqlRepo,
	}
}

func (s *streamService) ReportStreamQuality(ctx context.Context, req *model.StreamReportRequest) (*model.StreamReportResponse, error) {
	now := time.Now()
	status := model.DetermineStreamStatus(req.PacketLoss, req.FPS, req.Bitrate)

	controlSignal, err := s.redisRepo.GetControlSignal(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get control signal failed: %w", err)
	}

	if controlSignal == model.ControlActionStop {
		return s.buildStopResponse(req.RoomID), nil
	}

	if err := s.redisRepo.SetStreamQuality(ctx, req.RoomID, req, model.RedisStreamExpireSeconds); err != nil {
		return nil, fmt.Errorf("set stream quality to redis failed: %w", err)
	}

	onlineCount, err := s.simulateOnlineCount(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get online count failed: %w", err)
	}

	if err := s.redisRepo.SetOnlineCount(ctx, req.RoomID, onlineCount, model.RedisStreamExpireSeconds); err != nil {
		return nil, fmt.Errorf("set online count failed: %w", err)
	}

	info, err := s.redisRepo.GetStreamInfo(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get stream info failed: %w", err)
	}

	if info == nil {
		info = &model.StreamRealTimeInfo{
			RoomID:    req.RoomID,
			TeacherID: req.TeacherID,
			CourseID:  s.generateCourseID(),
		}

		session := &model.StreamSession{
			RoomID:      req.RoomID,
			TeacherID:   req.TeacherID,
			CourseID:    info.CourseID,
			Status:      status,
			StartTime:   now,
			OnlineCount: onlineCount,
		}

		if err := s.mysqlRepo.CreateStreamSession(ctx, session); err != nil {
			return nil, fmt.Errorf("create stream session failed: %w", err)
		}
	}

	info.Status = status
	info.OnlineCount = onlineCount
	info.PacketLoss = req.PacketLoss
	info.FPS = req.FPS
	info.Bitrate = req.Bitrate
	info.LastReportAt = now.Unix()

	if err := s.redisRepo.SetStreamInfo(ctx, info, model.RedisStreamExpireSeconds); err != nil {
		return nil, fmt.Errorf("set stream info to redis failed: %w", err)
	}

	if info != nil {
		activeSession, err := s.mysqlRepo.GetActiveSessionByRoomID(ctx, req.RoomID)
		if err != nil {
			return nil, fmt.Errorf("get active session failed: %w", err)
		}

		if activeSession != nil {
			prevStatus := activeSession.Status
			activeSession.Status = status
			activeSession.OnlineCount = onlineCount

			if prevStatus != status && status == model.StreamStatusDisconnected {
				endTime := now
				activeSession.EndTime = &endTime
				activeSession.StopReason = "stream disconnected"
			}

			if err := s.mysqlRepo.UpdateStreamSession(ctx, activeSession); err != nil {
				return nil, fmt.Errorf("update stream session failed: %w", err)
			}
		}
	}

	qualityLog := &model.StreamQualityLog{
		RoomID:       req.RoomID,
		TeacherID:    req.TeacherID,
		PacketLoss:   req.PacketLoss,
		FPS:          req.FPS,
		Bitrate:      req.Bitrate,
		Resolution:   req.Resolution,
		ReportedAt:   now,
		StreamStatus: status,
	}

	if err := s.mysqlRepo.CreateQualityLog(ctx, qualityLog); err != nil {
		return nil, fmt.Errorf("create quality log failed: %w", err)
	}

	resp := &model.StreamReportResponse{}
	resp.Code = 0
	resp.Message = "success"
	resp.Data.Status = status
	resp.Data.OnlineCount = onlineCount
	resp.Data.ReportedAt = now.Unix()

	return resp, nil
}

func (s *streamService) ControlStream(ctx context.Context, req *model.StreamControlRequest) (*model.StreamControlResponse, error) {
	now := time.Now()

	exists, err := s.redisRepo.CheckStreamExists(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("check stream exists failed: %w", err)
	}

	if !exists {
		return nil, fmt.Errorf("stream room %s not found or not active", req.RoomID)
	}

	if req.Action == model.ControlActionStop {
		if err := s.redisRepo.SetControlSignal(ctx, req.RoomID, model.ControlActionStop, 60); err != nil {
			return nil, fmt.Errorf("set stop control signal failed: %w", err)
		}

		if err := s.mysqlRepo.StopStreamSession(ctx, req.RoomID, req.Reason); err != nil {
			return nil, fmt.Errorf("stop stream session in mysql failed: %w", err)
		}

		if err := s.redisRepo.DeleteStreamInfo(ctx, req.RoomID); err != nil {
			return nil, fmt.Errorf("delete stream info from redis failed: %w", err)
		}
	} else if req.Action == model.ControlActionRestart {
		if err := s.redisRepo.DeleteControlSignal(ctx, req.RoomID); err != nil {
			return nil, fmt.Errorf("delete control signal failed: %w", err)
		}
	}

	controlLog := &model.StreamControlLog{
		RoomID:     req.RoomID,
		OperatorID: req.OperatorID,
		Action:     req.Action,
		Reason:     req.Reason,
		OperatedAt: now,
	}

	if err := s.mysqlRepo.CreateControlLog(ctx, controlLog); err != nil {
		return nil, fmt.Errorf("create control log failed: %w", err)
	}

	resp := &model.StreamControlResponse{}
	resp.Code = 0
	resp.Message = "success"
	resp.Data.RoomID = req.RoomID
	resp.Data.Action = req.Action
	resp.Data.OperatedAt = now.Unix()

	return resp, nil
}

func (s *streamService) GetStreamInfo(ctx context.Context, roomID string) (*model.StreamRealTimeInfo, error) {
	info, err := s.redisRepo.GetStreamInfo(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get stream info from redis failed: %w", err)
	}

	if info == nil {
		return nil, fmt.Errorf("stream room %s not found", roomID)
	}

	return info, nil
}

func (s *streamService) GetStreamQualityLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamQualityLog, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	logs, total, err := s.mysqlRepo.GetQualityLogsByRoomID(ctx, roomID, page, pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("get quality logs failed: %w", err)
	}

	return logs, total, nil
}

func (s *streamService) GetControlLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamControlLog, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	logs, total, err := s.mysqlRepo.GetControlLogs(ctx, roomID, page, pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("get control logs failed: %w", err)
	}

	return logs, total, nil
}

func (s *streamService) GetAllActiveStreams(ctx context.Context) ([]string, error) {
	roomIDs, err := s.redisRepo.GetAllActiveStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("get all active streams failed: %w", err)
	}

	return roomIDs, nil
}

func (s *streamService) simulateOnlineCount(ctx context.Context, roomID string) (int, error) {
	count, err := s.redisRepo.GetOnlineCount(ctx, roomID)
	if err != nil {
		return 0, err
	}

	if count == 0 {
		count = rand.Intn(50) + 10
	} else {
		change := rand.Intn(11) - 5
		count += change
		if count < 0 {
			count = 0
		}
	}

	return count, nil
}

func (s *streamService) buildStopResponse(roomID string) *model.StreamReportResponse {
	resp := &model.StreamReportResponse{}
	resp.Code = 1001
	resp.Message = "stream has been stopped by operator"
	resp.Data.Status = model.StreamStatusStopped
	resp.Data.OnlineCount = 0
	resp.Data.ReportedAt = time.Now().Unix()
	return resp
}

func (s *streamService) generateCourseID() uint64 {
	return uint64(rand.Int63n(10000) + 1000)
}
