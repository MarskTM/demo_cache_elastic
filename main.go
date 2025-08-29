package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
	"tool_cache/repo"
)

var (
	elaC      *repo.ElasticChannelParticipantsDAO
	redisC    *repo.ChannelParticipantsCacheDAO
	channelID int32
)

func init() {
	client := repo.ConnectElastic()
	elaC = repo.NewElasticChannelParticipantsDAO(client)
	redisC = repo.NewChannelParticipantsCacheDAO()

	channelID = int32(1001)

	// seed random (nếu không seed thì rand.Intn sẽ lặp giá trị giống nhau mỗi lần run)
	rand.Seed(time.Now().UnixNano())
}

func main() {
	// migrate dữ liệu
	// LoadAllData()

	fmt.Println("----------------------- Get Filter Data ----------------------------")
	_, total, err := elaC.SelectListAdmins(context.Background(), 1001, 10000, 0)
	if err != nil {
		fmt.Println("err get list: ", err)
	}
	fmt.Println("Get all Admin Total: ", total)
	// fmt.Println("Get all Admin: ", *getData)

	_, ok := redisC.GetList(channelID)
	if !ok {
		fmt.Println("err get list!")
	}

	redisC.GetString(channelID)
	// fmt.Println("Get all listID: ", listID)
}

// ===================================================================================
// sampleData tạo ra 10 documents mẫu
func sampleData(channelID int32, size int, idStart int) ([]repo.ElasticUserChannleParticipantsDO, []int32) {
	now := int32(time.Now().Unix())

	data := []repo.ElasticUserChannleParticipantsDO{}
	litsUserID := []int32{}

	for i := idStart; i <= size; i++ {
		doc := &repo.ElasticUserChannleParticipantsDO{
			ID:                int64(i),
			ChannelID:         channelID,
			UserID:            int64(i),
			IsCreator:         rand.Int31n(2),           // 0 hoặc 1
			AdminRights:       rand.Int31n(5),           // 0..4
			ParticipantType:   int8(rand.Intn(3) + 1),   // 1..3
			HiddenParticipant: int8(rand.Intn(2)),       // 0 hoặc 1
			IsLeft:            int8(rand.Intn(2)),       // 0 hoặc 1
			LeftAt:            now - rand.Int31n(10000), // thời điểm rời (nếu có)
			IsKicked:          int8(rand.Intn(2)),       // 0 hoặc 1
			BannedRights:      rand.Int31n(10),          // số random
			BannedUntilDate:   now + rand.Int31n(10000), // future
			Data: &repo.ChannelParticipantsDO{
				ID:                int64(i),
				ChannelID:         channelID,
				UserID:            int32(i),
				IsCreator:         rand.Int31n(5),
				ParticipantType:   int8(rand.Intn(3) + 1),
				InviterUserID:     int32(rand.Intn(9000) + 1000),
				InvitedAt:         now - rand.Int31n(10000),
				JoinedAt:          now - rand.Int31n(5000),
				IsWaitingAprrove:  int8(rand.Intn(2)),
				HiddenParticipant: int8(rand.Intn(2)),
				IsLeft:            int8(rand.Intn(2)),
				LeftAt:            now - rand.Int31n(10000),
				IsKicked:          int8(rand.Intn(2)),
				KickedBy:          int32(rand.Intn(9000) + 1000),
				KickedAt:          now - rand.Int31n(10000),
				HiddenPrehistory:  int8(rand.Intn(2)),
				AdminRights:       rand.Int31n(5),
				PromotedBy:        int32(rand.Intn(9000) + 1000),
				PromotedAt:        now - rand.Int31n(10000),
				Rank:              fmt.Sprintf("member-%d", rand.Intn(100)),
				BannedRights:      rand.Int31n(10),
				BannedUntilDate:   now + rand.Int31n(10000),
				BannedAt:          now - rand.Int31n(10000),
				ReadInboxMaxID:    rand.Int31n(10000),
				ReadOutboxMaxID:   rand.Int31n(10000),
				Date:              now - rand.Int31n(10000),
				State:             int8(rand.Intn(3)), // 0..2
				CreatedAt:         time.Now().Add(-time.Duration(rand.Intn(3600)) * time.Second).Format(time.RFC3339),
				UpdatedAt:         time.Now().Format(time.RFC3339),
			},
		}
		data = append(data, *doc)
		litsUserID = append(litsUserID, int32(doc.UserID))
	}

	return data, litsUserID
}

// Tạo mẫu dữ liệu và load lên elastic & redis
func LoadAllData() {
	fmt.Println("----------------------- Migrating Data ----------------------------")
	// Tạo data mẫu cho channel 1001
	docs, listUsers := sampleData(channelID, 500000, 1)
	// Lưu bulk vào Elasticsearch
	parts := make(chan *repo.ElasticUserChannleParticipantsDO, 4000)
	go func() {
		defer close(parts)
		for _, p := range docs {
			data := p
			parts <- &data
		}
	}()

	elaC.SaveAllDataBP(channelID, parts)
	// elaC.SetChannelVersion(1001, 1)
	redisC.SaveAllData(channelID, listUsers)
	redisC.SaveString(channelID, listUsers)

	meta, err := elaC.SelectMetaData(channelID)
	if err != nil {
		fmt.Println("LoadAllData Err: ", err)
	} else if meta == nil {
		fmt.Println("SelectMetaData is nil")
	} else {
		fmt.Println("SelectMetaData: ", *meta)
	}
}

// Thêm người dùng
func AddUsser() {
	fmt.Println("----------------------- Test Add User Scenario ----------------------------")
	newUsers, _ := sampleData(channelID, 10, 500001)
	newChannlParticipant := repo.ElasticChannleParticipantsDO{
		Version:      10,
		Participants: newUsers,
	}
	elaC.AddOrUpsertData(channelID, &newChannlParticipant)

	meta, err := elaC.SelectMetaData(channelID)
	if err != nil {
		fmt.Println("LoadAllData Err: ", err)
	}
	fmt.Println("SelectMetaData: ", *meta)
}

// Triển khai kịch bản xóa người dùng:
// - Xóa 1 người dùng
// - Xóa nhiều người
func DeleteUser() {}

// Triển khai kịch bản cập nhật:
// - Cập nhật thông tin của user mà không update version
// - Cập nhật thông tin của users cùng version
func UpdateUser() {}

// Triển khai kịch bản
func GetData() {}
