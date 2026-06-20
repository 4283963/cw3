package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cw3/internal/model"
	mysqlpkg "cw3/internal/pkg/mysql"

	"gorm.io/gorm"
)

type streamMySQLRepository struct {
	db *mysqlpkg.DB
}

func NewStreamMySQLRepository(db *mysqlpkg.DB) StreamMySQLRepository {
	return &streamMySQLRepository{
		db: db,
	}
}

func (r *streamMySQLRepository) CreateStreamSession(ctx context.Context, session *model.StreamSession) error {
	if err := r.db.WithContext(ctx).Create(session).Error; err != nil {
		return fmt.Errorf("create stream session failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) FirstOrCreateActiveSession(ctx context.Context, roomID string, template *model.StreamSession) (*model.StreamSession, bool, error) {
	var existing model.StreamSession
	err := r.db.WithContext(ctx).
		Where("room_id = ? AND status IN ?", roomID, []model.StreamStatus{
			model.StreamStatusLive,
			model.StreamStatusLagging,
		}).
		Order("start_time DESC").
		First(&existing).Error

	if err == nil {
		return &existing, false, nil
	}

	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, fmt.Errorf("query active session failed: %w", err)
	}

	template.RoomID = roomID

	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var locked model.StreamSession
		txErr := tx.Set("gorm:query_option", "FOR UPDATE").
			Where("room_id = ? AND status IN ?", roomID, []model.StreamStatus{
				model.StreamStatusLive,
				model.StreamStatusLagging,
			}).
			Order("start_time DESC").
			First(&locked).Error

		if txErr == nil {
			*template = locked
			return nil
		}

		if !errors.Is(txErr, gorm.ErrRecordNotFound) {
			return txErr
		}

		if createErr := tx.Create(template).Error; createErr != nil {
			return createErr
		}

		return nil
	})

	if err != nil {
		return nil, false, fmt.Errorf("first or create session transaction failed: %w", err)
	}

	return template, true, nil
}

func (r *streamMySQLRepository) UpdateStreamSession(ctx context.Context, session *model.StreamSession) error {
	if err := r.db.WithContext(ctx).Save(session).Error; err != nil {
		return fmt.Errorf("update stream session failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) GetActiveSessionByRoomID(ctx context.Context, roomID string) (*model.StreamSession, error) {
	var session model.StreamSession
	err := r.db.WithContext(ctx).
		Where("room_id = ? AND status IN ?", roomID, []model.StreamStatus{
			model.StreamStatusLive,
			model.StreamStatusLagging,
		}).
		Order("start_time DESC").
		First(&session).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get active session failed: %w", err)
	}

	return &session, nil
}

func (r *streamMySQLRepository) StopStreamSession(ctx context.Context, roomID, reason string) error {
	now := time.Now()
	result := r.db.WithContext(ctx).
		Model(&model.StreamSession{}).
		Where("room_id = ? AND status IN ?", roomID, []model.StreamStatus{
			model.StreamStatusLive,
			model.StreamStatusLagging,
			model.StreamStatusDisconnected,
		}).
		Updates(map[string]interface{}{
			"status":      model.StreamStatusStopped,
			"end_time":    &now,
			"stop_reason": reason,
		})

	if result.Error != nil {
		return fmt.Errorf("stop stream session failed: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return fmt.Errorf("no active session found for room %s", roomID)
	}

	return nil
}

func (r *streamMySQLRepository) CreateQualityLog(ctx context.Context, log *model.StreamQualityLog) error {
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		return fmt.Errorf("create quality log failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) BatchCreateQualityLogs(ctx context.Context, logs []*model.StreamQualityLog) error {
	if len(logs) == 0 {
		return nil
	}

	if err := r.db.WithContext(ctx).Create(logs).Error; err != nil {
		return fmt.Errorf("batch create quality logs failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) GetQualityLogsByRoomID(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamQualityLog, int64, error) {
	var logs []*model.StreamQualityLog
	var total int64

	query := r.db.WithContext(ctx).Model(&model.StreamQualityLog{}).Where("room_id = ?", roomID)

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count quality logs failed: %w", err)
	}

	offset := (page - 1) * pageSize
	if err := query.Offset(offset).Limit(pageSize).Order("reported_at DESC").Find(&logs).Error; err != nil {
		return nil, 0, fmt.Errorf("get quality logs failed: %w", err)
	}

	return logs, total, nil
}

func (r *streamMySQLRepository) CreateControlLog(ctx context.Context, log *model.StreamControlLog) error {
	if err := r.db.WithContext(ctx).Create(log).Error; err != nil {
		return fmt.Errorf("create control log failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) GetControlLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.StreamControlLog, int64, error) {
	var logs []*model.StreamControlLog
	var total int64

	query := r.db.WithContext(ctx).Model(&model.StreamControlLog{})
	if roomID != "" {
		query = query.Where("room_id = ?", roomID)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count control logs failed: %w", err)
	}

	offset := (page - 1) * pageSize
	if err := query.Offset(offset).Limit(pageSize).Order("operated_at DESC").Find(&logs).Error; err != nil {
		return nil, 0, fmt.Errorf("get control logs failed: %w", err)
	}

	return logs, total, nil
}

func (r *streamMySQLRepository) BatchCreateCDNSwitchLogs(ctx context.Context, logs []*model.CDNSwitchLog) error {
	if len(logs) == 0 {
		return nil
	}

	if err := r.db.WithContext(ctx).Create(logs).Error; err != nil {
		return fmt.Errorf("batch create cdn switch logs failed: %w", err)
	}
	return nil
}

func (r *streamMySQLRepository) GetCDNSwitchLogs(ctx context.Context, roomID string, page, pageSize int) ([]*model.CDNSwitchLog, int64, error) {
	var logs []*model.CDNSwitchLog
	var total int64

	query := r.db.WithContext(ctx).Model(&model.CDNSwitchLog{})
	if roomID != "" {
		query = query.Where("room_id = ?", roomID)
	}

	if err := query.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count cdn switch logs failed: %w", err)
	}

	offset := (page - 1) * pageSize
	if err := query.Offset(offset).Limit(pageSize).Order("switched_at DESC").Find(&logs).Error; err != nil {
		return nil, 0, fmt.Errorf("get cdn switch logs failed: %w", err)
	}

	return logs, total, nil
}
