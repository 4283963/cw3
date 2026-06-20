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

const (
	RedisLockPrefix     = "edu:stream:lock:"
	RedisDefaultLockTTL = 5
)

var (
	luaReleaseLock = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	luaCASSetStreamInfo = redis.NewScript(`
		local key = KEYS[1]
		local expect_ts = tonumber(ARGV[1])
		local new_value = ARGV[2]
		local ttl = tonumber(ARGV[3])

		local current = redis.call("GET", key)
		if current == false then
			if expect_ts == 0 then
				redis.call("SET", key, new_value, "EX", ttl)
				return 1
			else
				return 0
			end
		end

		local cjson_ok, decoded = pcall(cjson.decode, current)
		if not cjson_ok then
			redis.call("SET", key, new_value, "EX", ttl)
			return 1
		end

		local current_ts = tonumber(decoded["last_report_at"] or 0)
		if current_ts == expect_ts then
			redis.call("SET", key, new_value, "EX", ttl)
			return 1
		end

		return 0
	`)

	luaUpdateOnlineCount = redis.NewScript(`
		local key = KEYS[1]
		local ttl = tonumber(ARGV[1])

		local current = redis.call("GET", key)
		local count = 0

		if current == false then
			count = math.random(10, 60)
		else
			count = tonumber(current)
			local change = math.random(-5, 5)
			count = count + change
			if count < 0 then
				count = 0
			end
		end

		redis.call("SET", key, count, "EX", ttl)
		return count
	`)
)

type streamRedisRepository struct {
	client *redispkg.Client
}

func NewStreamRedisRepository(client *redispkg.Client) StreamRedisRepository {
	return &streamRedisRepository{
		client: client,
	}
}

func (r *streamRedisRepository) AcquireLock(ctx context.Context, lockKey string, requestID string, expireSec int) (bool, error) {
	fullKey := RedisLockPrefix + lockKey

	ok, err := r.client.SetNX(ctx, fullKey, requestID, time.Duration(expireSec)*time.Second).Result()
	if err != nil {
		return false, fmt.Errorf("acquire lock %s failed: %w", fullKey, err)
	}

	return ok, nil
}

func (r *streamRedisRepository) ReleaseLock(ctx context.Context, lockKey string, requestID string) (bool, error) {
	fullKey := RedisLockPrefix + lockKey

	res, err := luaReleaseLock.Run(ctx, r.client, []string{fullKey}, requestID).Result()
	if err != nil {
		return false, fmt.Errorf("release lock %s failed: %w", fullKey, err)
	}

	releaseOk, _ := res.(int64)
	return releaseOk == 1, nil
}

func (r *streamRedisRepository) SetStreamInfo(ctx context.Context, info *model.StreamRealTimeInfo, expireSec int) error {
	key := model.RedisKeyStreamInfo + info.RoomID
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal stream info failed: %w", err)
	}

	return r.client.Set(ctx, key, data, time.Duration(expireSec)*time.Second).Err()
}

func (r *streamRedisRepository) SetStreamInfoCAS(ctx context.Context, roomID string, info *model.StreamRealTimeInfo, expireSec int, expectLastReportAt int64) (bool, error) {
	key := model.RedisKeyStreamInfo + roomID

	info.RoomID = roomID

	data, err := json.Marshal(info)
	if err != nil {
		return false, fmt.Errorf("marshal stream info failed: %w", err)
	}

	res, err := luaCASSetStreamInfo.Run(ctx, r.client,
		[]string{key},
		expectLastReportAt, string(data), expireSec,
	).Result()
	if err != nil {
		return false, fmt.Errorf("cas set stream info failed: %w", err)
	}

	ok, _ := res.(int64)
	return ok == 1, nil
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

	if info.RoomID != "" && info.RoomID != roomID {
		return nil, fmt.Errorf("room id mismatch: key=%s stored_room=%s", roomID, info.RoomID)
	}

	info.RoomID = roomID

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

func (r *streamRedisRepository) UpdateOnlineCountAtomic(ctx context.Context, roomID string, expireSec int) (int, error) {
	key := model.RedisKeyOnlineCount + roomID

	res, err := luaUpdateOnlineCount.Run(ctx, r.client, []string{key}, expireSec).Result()
	if err != nil {
		return 0, fmt.Errorf("atomic update online count failed: %w", err)
	}

	count, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected result type from lua script")
	}

	return int(count), nil
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
