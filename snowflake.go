package distributecache

import (
	"fmt"
	"hash/fnv"
	"time"
)

const (
	epoch          = int64(1719792000000) // 定义起始时间戳 (可选择具体时间)
	nodeBits       = uint(10)             // 节点ID占用的位数
	sequenceBits   = uint(12)             // 序列号占用的位数
	maxNodeID      = int64(-1 ^ (-1 << nodeBits))
	maxSequence    = int64(-1 ^ (-1 << sequenceBits))
	nodeShift      = sequenceBits
	timestampShift = sequenceBits + nodeBits
)

type Snowflake struct {
	lastTimestamp int64
	nodeID        int64
	sequence      int64
}

func NewSnowflake(nodeID int64) (*Snowflake, error) {
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("node id must be between 0 and %d", maxNodeID)
	}
	return &Snowflake{
		lastTimestamp: 0,
		nodeID:        nodeID,
		sequence:      0,
	}, nil
}

func (s *Snowflake) Generate() int64 {
	timestamp := currentTimestamp()

	if timestamp == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			for timestamp <= s.lastTimestamp {
				timestamp = currentTimestamp()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = timestamp

	return ((timestamp - epoch) << timestampShift) |
		(s.nodeID << nodeShift) |
		s.sequence
}

func currentTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// getNodeID 根据 IP 地址和端口号生成唯一的 nodeID
func getNodeID(ip string, port string) int64 {
	// 拼接 IP 地址和端口号
	addr := fmt.Sprintf("%s:%s", ip, port)

	// 计算哈希值
	h := fnv.New32a()
	h.Write([]byte(addr))

	// 将哈希值取模，确保 nodeID 在 0 到 maxNodeID 之间
	nodeID := int64(h.Sum32()) % maxNodeID

	return nodeID
}

func GenerateID(ip string, port string) int64 {
	nodeID := getNodeID(ip, port)

	snowflake, _ := NewSnowflake(nodeID)

	return snowflake.Generate()
}
