package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cw3/internal/model"
	redispkg "cw3/internal/pkg/redis"

	"github.com/go-redis/redis/v8"
)

type streamRedisRepository struct {
	client *redispkg.Client
}

func NewStreamRedisRepository(client *redispkg.Client) StreamRedisRepository {
	return &streamRedisRepository{
		client: client,
	}
}

func (r *streamRedisRepository) SetStreamInfo(ctx context.Context, info *model.StreamRealTimeInfo, expireSec int) error {
	key := model.RedisKeyStreamInfo + info.RoomID
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal stream info failed: %w", err)
	}

	return r.client.Set(ctx, key, data, time.Duration(expireSec)*time.Second).Err()
}

func (r *streamRedisRepository) GetStreamInfo(ctx context.Context, roomID string) (*model.StreamRealTimeInfo, error) {
	key := model.RedisKeyStreamInfo + roomID
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("get stream info failed: %w", err)
	}

	var info model.StreamRealTimeInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("unmarshal stream info failed: %w", err)
	}

	return &info, nil
}

func (r *streamRedisRepository) DeleteStreamInfo(ctx context.Context, roomID string) error {
	key := model.RedisKeyStreamInfo + roomID
	return r.client.Del(ctx, key).Err()
}

func (r *streamRedisRepository) SetOnlineCount(ctx context.Context, roomID string, count int, expireSec int) error {
	key := model.RedisKeyOnlineCount + roomID
	return r.client.Set(ctx, key, count, time.Duration(expireSec)*time.Second).Err()
}

func (r *streamRedisRepository) GetOnlineCount(ctx context.Context, roomID string) (int, error) {
	key := model.RedisKeyOnlineCount + roomID
	count, err := r.client.Get(ctx, key).Int()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("get online count failed: %w", err)
	}
	return count, nil
}

func (r *streamRedisRepository) IncrOnlineCount(ctx context.Context, roomID string) (int64, error) {
	key := model.RedisKeyOnlineCount + roomID
	return r.client.Incr(ctx, key).Result()
}

func (r *streamRedisRepository) DecrOnlineCount(ctx context.Context, roomID string) (int64, error) {
	key := model.RedisKeyOnlineCount + roomID
	return r.client.Decr(ctx, key).Result()
}

func (r *streamRedisRepository) SetControlSignal(ctx context.Context, roomID string, action string, expireSec int) error {
	key := model.RedisKeyStreamControl + roomID
	return r.client.Set(ctx, key, action, time.Duration(expireSec)*time.Second).Err()
}

func (r *streamRedisRepository) GetControlSignal(ctx context.Context, roomID string) (string, error) {
	key := model.RedisKeyStreamControl + roomID
	action, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", fmt.Errorf("get control signal failed: %w", err)
	}
	return action, nil
}

func (r *streamRedisRepository) DeleteControlSignal(ctx context.Context, roomID string) error {
	key := model.RedisKeyStreamControl + roomID
	return r.client.Del(ctx, key).Err()
}

func (r *streamRedisRepository) SetStreamQuality(ctx context.Context, roomID string, report *model.StreamReportRequest, expireSec int) error {
	key := model.RedisKeyStreamQuality + roomID
	data, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("marshal stream quality failed: %w", err)
	}

	return r.client.Set(ctx, key, data, time.Duration(expireSec)*time.Second).Err()
}

func (r *streamRedisRepository) GetStreamQuality(ctx context.Context, roomID string) (*model.StreamReportRequest, error) {
	key := model.RedisKeyStreamQuality + roomID
	data, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("get stream quality failed: %w", err)
	}

	var report model.StreamReportRequest
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("unmarshal stream quality failed: %w", err)
	}

	return &report, nil
}

func (r *streamRedisRepository) CheckStreamExists(ctx context.Context, roomID string) (bool, error) {
	key := model.RedisKeyStreamInfo + roomID
	exists, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("check stream exists failed: %w", err)
	}
	return exists > 0, nil
}

func (r *streamRedisRepository) GetAllActiveStreams(ctx context.Context) ([]string, error) {
	pattern := model.RedisKeyStreamInfo + "*"
	var roomIDs []string
	var cursor uint64

	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan stream keys failed: %w", err)
		}

		for _, key := range keys {
			roomID := key[len(model.RedisKeyStreamInfo):]
			roomIDs = append(roomIDs, roomID)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return roomIDs, nil
}
