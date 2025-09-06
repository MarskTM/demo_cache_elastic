package repo

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type ChannelParticipantsCacheDAO struct {
	conn *redis.Client
}

func NewChannelParticipantsCacheDAO() *ChannelParticipantsCacheDAO {
	rdb := ConnectRedis()
	return &ChannelParticipantsCacheDAO{conn: rdb}
}

func (r *ChannelParticipantsCacheDAO) SaveAllData(channelID int32, listUsers []int32) bool {
	timeStart := time.Now()
	if r == nil || r.conn == nil {
		log.Printf("redis client is nil")
		return false
	}
	key := fmt.Sprintf("channel:%d:participants", channelID)

	// Xóa key cũ để reset toàn bộ
	if err := r.conn.Del(key).Err(); err != nil {
		log.Printf("Redis DEL error: %v", err)
		return false
	}

	// chuyển []int32 → []interface{}
	members := make([]interface{}, len(listUsers))
	for i, u := range listUsers {
		members[i] = u
	}

	// SADD: thêm toàn bộ user mới vào set
	if err := r.conn.SAdd(key, members...).Err(); err != nil {
		log.Printf("Redis SAdd error: %v", err)
		return false
	}

	// log.Printf("✅ Redis Reset and Inserted %d users into %s", len(listUsers), key)

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm SaveAllData: %s\n", duration)
	return true
}

func (r *ChannelParticipantsCacheDAO) GetList(channelID int32) ([]int32, bool) {
	timeStart := time.Now()

	if r == nil || r.conn == nil {
		log.Printf("redis client is nil")
		return nil, false
	}

	key := fmt.Sprintf("channel:%d:participants", channelID)

	// Kiểm tra key có tồn tại không
	exists, err := r.conn.Exists(key).Result()
	if err != nil {
		log.Printf("Redis EXISTS error: %v", err)
		return nil, false
	}
	if exists == 0 {
		// Key chưa có
		return nil, false
	}

	// Lấy toàn bộ members
	members, err := r.conn.SMembers(key).Result()
	if err != nil {
		log.Printf("Redis SMEMBERS error: %v", err)
		return nil, false
	}

	// Convert []string -> []int32
	out := make([]int32, 0, len(members))
	for _, s := range members {
		// member có thể là user_id dạng string. Nếu bạn đang SADD số nguyên trực tiếp,
		// go-redis cũng serialize thành string.
		v, convErr := strconv.ParseInt(s, 10, 32)
		if convErr != nil {
			// Bỏ qua phần tử lỗi (hoặc bạn có thể return lỗi)
			log.Printf("parse member '%s' to int32 error: %v", s, convErr)
			continue
		}
		out = append(out, int32(v))
	}

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm GetList: %s\n", duration)
	return out, true
}

func (r *ChannelParticipantsCacheDAO) DeleteUsers(channelID int32, userIDs []int32) bool {
	timeStart := time.Now()
	if r == nil || r.conn == nil {
		log.Printf("redis client is nil")
		return false
	}
	if len(userIDs) == 0 {
		// Không có gì để xóa → coi như thành công
		return true
	}

	key := fmt.Sprintf("channel:%d:participants", channelID)

	// Chuẩn bị args cho SREM: []int32 -> []interface{}
	members := make([]interface{}, 0, len(userIDs))
	for _, id := range userIDs {
		members = append(members, id)
	}

	// Xóa và kiểm tra còn lại bao nhiêu phần tử
	pipe := r.conn.TxPipeline()
	srem := pipe.SRem(key, members...) // *IntCmd: số members thực sự bị xóa
	scard := pipe.SCard(key)           // *IntCmd: số lượng còn lại
	if _, err := pipe.Exec(); err != nil {
		log.Printf("Redis pipeline SREM/SCARD error: %v", err)
		return false
	}

	removed := srem.Val()
	remain := scard.Val()
	log.Printf("SREM %d users from %s -> removed=%d, remain=%d", len(userIDs), key, removed, remain)

	// // Nếu set rỗng, xóa key để gọn dữ liệu (không bắt buộc)
	// if remain == 0 {
	// 	// Dùng UNLINK để tránh block; có thể dùng DEL nếu muốn đồng bộ
	// 	if err := r.conn.Unlink(key).Err(); err != nil {
	// 		log.Printf("Redis UNLINK %s error: %v", key, err)
	// 		// Không coi là fail nghiêm trọng
	// 	} else {
	// 		log.Printf("UNLINK key %s (set empty)", key)
	// 	}
	// }

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm DeleteUsers: %s\n", duration)
	return true
}

func (r *ChannelParticipantsCacheDAO) AddUsers(channelID int32, userIDs []int32) bool {
	timeStart := time.Now()
	if r == nil || r.conn == nil {
		log.Printf("redis client is nil")
		return false
	}
	key := fmt.Sprintf("channel:%d:participants", channelID)

	if len(userIDs) == 0 {
		log.Printf("⚠️ AddUsers: empty input for key %s, skip SADD", key)
		return true
	}

	// chuyển []int32 → []interface{}
	members := make([]interface{}, len(userIDs))
	for i, u := range userIDs {
		members[i] = u
	}

	// SADD: thêm toàn bộ user mới vào set
	if err := r.conn.SAdd(key, members...).Err(); err != nil {
		log.Printf("Redis SAdd error: %v", err)
		return false
	}

	log.Printf("✅ Redis Upserted %d users into %s", len(userIDs), key)

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm AddUsers: %s\n", duration)
	return true
}

// key: channel:<id>:participants:str
func (r *ChannelParticipantsCacheDAO) SaveString(channelID int32, userIDs []int32) error {
	timeStart := time.Now()
	if r == nil || r.conn == nil {
		return fmt.Errorf("redis client is nil")
	}
	key := fmt.Sprintf("channel:%d:participants:str", channelID)

	// Nếu key đã tồn tại thì xóa trước để "reset"
	if exists, err := r.conn.Exists(key).Result(); err == nil && exists > 0 {
		if delErr := r.conn.Del(key).Err(); delErr != nil {
			return fmt.Errorf("redis DEL error: %w", delErr)
		}
	} else if err != nil {
		return fmt.Errorf("redis EXISTS error: %w", err)
	}

	// Build CSV trong memory
	var b strings.Builder
	b.Grow(len(userIDs) * 11) // ước lượng dung lượng

	for i, id := range userIDs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(int64(id), 10))
	}

	if err := r.conn.Set(key, b.String(), 0).Err(); err != nil {
		return fmt.Errorf("redis SET error: %w", err)
	}
	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm SaveString: %s\n", duration)
	return nil
}

func (r *ChannelParticipantsCacheDAO) GetString(channelID int32) ([]int32, error) {
	timeStart := time.Now()

	if r == nil || r.conn == nil {
		return nil, fmt.Errorf("redis client is nil")
	}
	key := fmt.Sprintf("channel:%d:participants:str", channelID)

	raw, err := r.conn.Get(key).Result()
	if err == redis.Nil {
		return nil, nil // chưa có key
	}
	if err != nil {
		return nil, fmt.Errorf("redis GET error: %w", err)
	}
	if raw == "" {
		return []int32{}, nil
	}

	parts := strings.Split(raw, ",")
	out := make([]int32, 0, len(parts))
	for _, s := range parts {
		if s == "" {
			continue
		}
		v, convErr := strconv.ParseInt(s, 10, 32)
		if convErr != nil {
			// có thể skip hoặc return error, tuỳ bạn
			continue
		}
		out = append(out, int32(v))
	}
	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm GetString: %s\n", duration)
	return out, nil
}

func (r *ChannelParticipantsCacheDAO) AddUsersString(channelID int32, userIDs []int32) error {
	timeStart := time.Now()
	if r == nil || r.conn == nil {
		return fmt.Errorf("redis client is nil")
	}
	key := fmt.Sprintf("channel:%d:participants:str", channelID)

	// Lấy dữ liệu cũ từ Redis
	raw, err := r.conn.Get(key).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("redis GET error: %w", err)
	}

	// Parse dữ liệu cũ thành map để check trùng
	existing := make(map[int32]struct{})
	if raw != "" {
		parts := strings.Split(raw, ",")
		for _, s := range parts {
			if s == "" {
				continue
			}
			v, convErr := strconv.ParseInt(s, 10, 32)
			if convErr == nil {
				existing[int32(v)] = struct{}{}
			}
		}
	}

	// Thêm userIDs mới (nếu chưa có)
	for _, id := range userIDs {
		if _, ok := existing[id]; !ok {
			existing[id] = struct{}{}
		}
	}

	// Build lại CSV từ map
	var b strings.Builder
	b.Grow(len(existing) * 11)
	first := true
	for id := range existing {
		if !first {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(int64(id), 10))
		first = false
	}

	// Lưu lại vào Redis
	if err := r.conn.Set(key, b.String(), 0).Err(); err != nil {
		return fmt.Errorf("redis SET error: %w", err)
	}

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm AddUsersString: %s\n", duration)
	return nil
}

func (r *ChannelParticipantsCacheDAO) DeleteString(channelID int32, userIDs []int32) error {
	timeStart := time.Now()

	if r == nil || r.conn == nil {
		return fmt.Errorf("redis client is nil")
	}
	key := fmt.Sprintf("channel:%d:participants:str", channelID)

	raw, err := r.conn.Get(key).Result()
	if err == redis.Nil {
		return nil // chưa có key
	}
	if err != nil {
		return fmt.Errorf("redis GET error: %w", err)
	}
	if raw == "" {
		return nil
	}

	// Parse thành map để xóa nhanh
	parts := strings.Split(raw, ",")
	out := make(map[int32]struct{}, len(parts))
	for _, s := range parts {
		if s == "" {
			continue
		}
		v, convErr := strconv.ParseInt(s, 10, 32)
		if convErr != nil {
			continue
		}
		out[int32(v)] = struct{}{}
	}

	// Xóa các userIDs cần remove
	for _, id := range userIDs {
		delete(out, id)
	}

	// Build lại CSV string từ map
	if len(out) == 0 {
		// Nếu không còn user nào → xóa luôn key
		if delErr := r.conn.Del(key).Err(); delErr != nil {
			return fmt.Errorf("redis DEL error: %w", delErr)
		}
	} else {
		var b strings.Builder
		b.Grow(len(out) * 11)

		first := true
		for id := range out {
			if !first {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatInt(int64(id), 10))
			first = false
		}

		if setErr := r.conn.Set(key, b.String(), 0).Err(); setErr != nil {
			return fmt.Errorf("redis SET error: %w", setErr)
		}
	}

	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm DeleteString: %s\n", duration)
	return nil
}

// ConnectRedis khởi tạo kết nối Redis (go-redis cũ, không dùng context trong Ping()).
func ConnectRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379", // host:port
		Password:     "",               // để trống nếu không có password
		DB:           0,                // 0–15
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	// test ping (KHÔNG context với go-redis import cũ)
	if _, err := rdb.Ping().Result(); err != nil {
		log.Fatalf("Không kết nối được Redis: %v", err)
	} else {
		fmt.Println("✅ Kết nối Redis thành công")
	}
	return rdb
}
