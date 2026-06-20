package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	mrand "math/rand"
	"sync"
	"time"

	"cw3/internal/model"
	"cw3/internal/repository"
)

const (
	maxLockRetries    = 20
	lockRetryInterval = 50 * time.Millisecond
	lockTTLSeconds    = 10
	casMaxRetries     = 5
)

type lockedRand struct {
	mu sync.Mutex
	r  *mrand.Rand
}

func newLockedRand() *lockedRand {
	seed := time.Now().UnixNano()
	return &lockedRand{
		r: mrand.New(mrand.NewSource(seed)),
	}
}

func (lr *lockedRand) Intn(n int) int {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.r.Intn(n)
}

func (lr *lockedRand) Int63n(n int64) int64 {
	lr.mu.Lock()
	defer lr.mu.Unlock()
	return lr.r.Int63n(n)
}

type streamService struct {
	redisRepo repository.StreamRedisRepository
	mysqlRepo repository.StreamMySQLRepository
	lr        *lockedRand
}

func NewStreamService(redisRepo repository.StreamRedisRepository, mysqlRepo repository.StreamMySQLRepository) StreamService {
	return &streamService{
		redisRepo: redisRepo,
		mysqlRepo: mysqlRepo,
		lr:        newLockedRand(),
	}
}

func generateRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), mrand.Intn(100000))
	}
	return hex.EncodeToString(b)
}

func (s *streamService) acquireRoomLock(ctx context.Context, roomID string) (string, error) {
	requestID := generateRequestID()
	lockKey := "room:" + roomID

	for i := 0; i < maxLockRetries; i++ {
		ok, err := s.redisRepo.AcquireLock(ctx, lockKey, requestID, lockTTLSeconds)
		if err != nil {
			return "", fmt.Errorf("acquire lock error: %w", err)
		}
		if ok {
			return requestID, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(lockRetryInterval):
		}
	}

	return "", fmt.Errorf("failed to acquire lock for room %s after %d retries", roomID, maxLockRetries)
}

func (s *streamService) releaseRoomLock(ctx context.Context, roomID string, requestID string) {
	lockKey := "room:" + roomID
	_, _ = s.redisRepo.ReleaseLock(ctx, lockKey, requestID)
}

func (s *streamService) ReportStreamQuality(ctx context.Context, req *model.StreamReportRequest) (*model.StreamReportResponse, error) {
	now := time.Now()
	nowUnix := now.Unix()
	status := model.DetermineStreamStatus(req.PacketLoss, req.FPS, req.Bitrate)

	controlSignal, err := s.redisRepo.GetControlSignal(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get control signal failed: %w", err)
	}
	if controlSignal == model.ControlActionStop {
		return s.buildStopResponse(req.RoomID), nil
	}

	requestID, err := s.acquireRoomLock(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("room lock timeout: %w", err)
	}
	defer s.releaseRoomLock(ctx, req.RoomID, requestID)

	if err := s.redisRepo.SetStreamQuality(ctx, req.RoomID, req, model.RedisStreamExpireSeconds); err != nil {
		return nil, fmt.Errorf("set stream quality to redis failed: %w", err)
	}

	onlineCount, err := s.redisRepo.UpdateOnlineCountAtomic(ctx, req.RoomID, model.RedisStreamExpireSeconds)
	if err != nil {
		return nil, fmt.Errorf("update online count failed: %w", err)
	}

	info, err := s.redisRepo.GetStreamInfo(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get stream info failed: %w", err)
	}

	expectLastReportAt := int64(0)
	var session *model.StreamSession

	if info == nil {
		courseID := s.generateCourseID()

		sessionTemplate := &model.StreamSession{
			RoomID:      req.RoomID,
			TeacherID:   req.TeacherID,
			CourseID:    courseID,
			Status:      status,
			StartTime:   now,
			OnlineCount: onlineCount,
		}

		session, _, err = s.mysqlRepo.FirstOrCreateActiveSession(ctx, req.RoomID, sessionTemplate)
		if err != nil {
			return nil, fmt.Errorf("first or create session failed: %w", err)
		}

		info = &model.StreamRealTimeInfo{
			RoomID:       req.RoomID,
			TeacherID:    session.TeacherID,
			CourseID:     session.CourseID,
			Status:       status,
			OnlineCount:  onlineCount,
			PacketLoss:   req.PacketLoss,
			FPS:          req.FPS,
			Bitrate:      req.Bitrate,
			LastReportAt: nowUnix,
			CDNLine:      model.CDNLinePrimary,
			CDNURL:       model.DefaultPrimaryCDNURL,
		}
		expectLastReportAt = 0
	} else {
		expectLastReportAt = info.LastReportAt

		info.RoomID = req.RoomID
		info.Status = status
		info.OnlineCount = onlineCount
		info.PacketLoss = req.PacketLoss
		info.FPS = req.FPS
		info.Bitrate = req.Bitrate
		info.LastReportAt = nowUnix

		session, err = s.mysqlRepo.GetActiveSessionByRoomID(ctx, req.RoomID)
		if err != nil {
			return nil, fmt.Errorf("get active session failed: %w", err)
		}
	}

	casOk := false
	for i := 0; i < casMaxRetries; i++ {
		ok, casErr := s.redisRepo.SetStreamInfoCAS(ctx, req.RoomID, info, model.RedisStreamExpireSeconds, expectLastReportAt)
		if casErr != nil {
			return nil, fmt.Errorf("cas set stream info failed: %w", casErr)
		}
		if ok {
			casOk = true
			break
		}

		curInfo, getErr := s.redisRepo.GetStreamInfo(ctx, req.RoomID)
		if getErr != nil {
			return nil, fmt.Errorf("reget stream info for cas failed: %w", getErr)
		}
		if curInfo == nil {
			expectLastReportAt = 0
		} else {
			expectLastReportAt = curInfo.LastReportAt

			curInfo.Status = status
			curInfo.OnlineCount = onlineCount
			curInfo.PacketLoss = req.PacketLoss
			curInfo.FPS = req.FPS
			curInfo.Bitrate = req.Bitrate
			curInfo.LastReportAt = nowUnix
			info = curInfo
		}
	}

	if !casOk {
		return nil, fmt.Errorf("failed to update stream info after %d CAS retries", casMaxRetries)
	}

	if session != nil {
		prevStatus := session.Status
		session.Status = status
		session.OnlineCount = onlineCount

		if prevStatus != status && status == model.StreamStatusDisconnected {
			endTime := now
			session.EndTime = &endTime
			session.StopReason = "stream disconnected"
		}

		if err := s.mysqlRepo.UpdateStreamSession(ctx, session); err != nil {
			return nil, fmt.Errorf("update stream session failed: %w", err)
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
	resp.Data.ReportedAt = nowUnix

	return resp, nil
}

func (s *streamService) ControlStream(ctx context.Context, req *model.StreamControlRequest) (*model.StreamControlResponse, error) {
	now := time.Now()
	nowUnix := now.Unix()

	requestID, err := s.acquireRoomLock(ctx, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("room lock timeout: %w", err)
	}
	defer s.releaseRoomLock(ctx, req.RoomID, requestID)

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
	resp.Data.OperatedAt = nowUnix

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
	max := big.NewInt(10000)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return uint64(s.lr.Int63n(10000) + 1000)
	}
	return uint64(n.Int64()) + 1000
}

func (s *streamService) generateBatchID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("batch-%d-%d", time.Now().UnixNano(), s.lr.Int63n(1000000))
	}
	return "batch-" + hex.EncodeToString(b)
}

func (s *streamService) BatchSwitchCDN(ctx context.Context, req *model.BatchSwitchCDNRequest) (*model.BatchSwitchCDNResponse, error) {
	now := time.Now()
	nowUnix := now.Unix()

	targetLine := model.CDNLineType(req.TargetLine)
	if !targetLine.Valid() {
		return nil, fmt.Errorf("invalid target_line: %s", req.TargetLine)
	}

	var targetURL string
	if targetLine == model.CDNLineBackup {
		if req.BackupURL != "" {
			targetURL = req.BackupURL
		} else {
			targetURL = model.DefaultBackupCDNURL
		}
	} else {
		if req.PrimaryURL != "" {
			targetURL = req.PrimaryURL
		} else {
			targetURL = model.DefaultPrimaryCDNURL
		}
	}

	seen := make(map[string]struct{}, len(req.RoomIDs))
	uniqueRoomIDs := make([]string, 0, len(req.RoomIDs))
	for _, roomID := range req.RoomIDs {
		if _, ok := seen[roomID]; ok {
			continue
		}
		seen[roomID] = struct{}{}
		uniqueRoomIDs = append(uniqueRoomIDs, roomID)
	}

	batchID := s.generateBatchID()
	type switchContext struct {
		roomID   string
		fromLine model.CDNLineType
		fromURL  string
	}

	switchCtxs := make([]switchContext, 0, len(uniqueRoomIDs))
	var successItems []model.CDNSwitchResultItem
	var failedItems []model.CDNSwitchResultItem

	for _, roomID := range uniqueRoomIDs {
		requestID, err := s.acquireRoomLock(ctx, roomID)
		if err != nil {
			failedItems = append(failedItems, model.CDNSwitchResultItem{
				RoomID: roomID,
				Reason: "acquire room lock timeout",
			})
			continue
		}

		_, fromLine, fromURL, ok, err := s.redisRepo.UpdateCDNLine(ctx, roomID, targetLine, targetURL, model.RedisStreamExpireSeconds)
		s.releaseRoomLock(ctx, roomID, requestID)

		if err != nil {
			failedItems = append(failedItems, model.CDNSwitchResultItem{
				RoomID: roomID,
				Reason: "update cdn line failed: " + err.Error(),
			})
			continue
		}

		if !ok {
			failedItems = append(failedItems, model.CDNSwitchResultItem{
				RoomID: roomID,
				Reason: "stream not active or info missing",
			})
			continue
		}

		switchCtxs = append(switchCtxs, switchContext{
			roomID:   roomID,
			fromLine: fromLine,
			fromURL:  fromURL,
		})
		successItems = append(successItems, model.CDNSwitchResultItem{
			RoomID: roomID,
			CDNURL: targetURL,
		})
	}

	if len(switchCtxs) > 0 {
		logs := make([]*model.CDNSwitchLog, 0, len(switchCtxs))
		for _, sc := range switchCtxs {
			logs = append(logs, &model.CDNSwitchLog{
				RoomID:     sc.roomID,
				OperatorID: req.OperatorID,
				FromLine:   sc.fromLine,
				ToLine:     targetLine,
				FromURL:    sc.fromURL,
				ToURL:      targetURL,
				BatchID:    batchID,
				Reason:     req.Reason,
				SwitchedAt: now,
			})
		}

		if err := s.mysqlRepo.BatchCreateCDNSwitchLogs(ctx, logs); err != nil {
			return nil, fmt.Errorf("batch create cdn switch logs failed: %w", err)
		}
	}

	resp := &model.BatchSwitchCDNResponse{}
	resp.Code = 0
	resp.Message = "success"
	resp.Data.SwitchedAt = nowUnix
	resp.Data.TargetLine = targetLine
	resp.Data.Success = successItems
	resp.Data.Failed = failedItems
	resp.Data.SuccessCount = len(successItems)
	resp.Data.FailedCount = len(failedItems)

	return resp, nil
}

func (s *streamService) GetCDNSwitchLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.CDNSwitchLog, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	logs, total, err := s.mysqlRepo.GetCDNSwitchLogs(ctx, roomID, page, pageSize)
	if err != nil {
		return nil, 0, fmt.Errorf("get cdn switch logs failed: %w", err)
	}

	return logs, total, nil
}
