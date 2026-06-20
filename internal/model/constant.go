package model

const (
	RedisKeyPrefix        = "edu:stream:"
	RedisKeyStreamInfo    = RedisKeyPrefix + "info:"
	RedisKeyOnlineCount   = RedisKeyPrefix + "online:"
	RedisKeyStreamQuality = RedisKeyPrefix + "quality:"
	RedisKeyStreamControl = RedisKeyPrefix + "control:"

	RedisStreamExpireSeconds = 300
)

const (
	DefaultTargetFPS     = 30
	DefaultMinFPS        = 15
	DefaultMaxPacketLoss = 5.0
	DefaultMinBitrate    = 500
)

const (
	ControlActionStop    = "stop"
	ControlActionRestart = "restart"
)

func (s StreamStatus) String() string {
	return string(s)
}

func DetermineStreamStatus(packetLoss float64, fps, bitrate int) StreamStatus {
	if fps <= 0 || bitrate <= 0 {
		return StreamStatusDisconnected
	}

	if packetLoss > DefaultMaxPacketLoss || fps < DefaultMinFPS || bitrate < DefaultMinBitrate {
		return StreamStatusLagging
	}

	return StreamStatusLive
}
