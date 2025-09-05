package main

import (
	"fmt"
	"math/rand"
	"sync"
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

	// Lấy dữ liệu
	GetData()

	// Cập nhật dữ liệu
	UpdateUser()
}

// ===================================================================================
// sampleData tạo ra 10 documents mẫu
func sampleData(channelID int32, size int, idStart int) ([]repo.ElasticChannelParticipantsDO, []int32) {
	now := int32(time.Now().Unix())

	data := []repo.ElasticChannelParticipantsDO{}
	litsUserID := []int32{}

	for i := idStart; i <= size; i++ {
		doc := &repo.ElasticChannelParticipantsDO{
			ID:                int64(i),
			ChannelID:         channelID,
			UserID:            int32(i),
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
	timeStart := time.Now()
	wg := sync.WaitGroup{}

	/*
		// Tạo group data mẫu cho channel
		500K user/goroutine - process: 3 	time: 33962 ms
		100K user/goroutine - process: 10 	time: 18696 ms
		60K  user/goroutine - process: 20 	time  27325 ms
	*/

	// Tạo group data mẫu cho channel
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			docs, listUsers := sampleData(channelID, 500000, 1)
			err := elaC.SaveAllUsers(channelID+int32(i), -1, docs)
			if err != nil {
				fmt.Println("SaveAllUsers Err: ", err)
				return
			}

			redisC.SaveAllData(channelID+int32(i), listUsers)
			redisC.SaveString(channelID+int32(i), listUsers)
		}(i)
	}
	wg.Wait()
	fmt.Println("Migrate data completed! Time to Migrate Data: ", time.Since(timeStart).Milliseconds(), "ms")
}

// Thêm người dùng
// func AddUsser() {
// 	fmt.Println("----------------------- Test Add User Scenario ----------------------------")
// 	newUsers, _ := sampleData(channelID, 10, 500001)
// 	newChannlParticipant := repo.ElasticChannleParticipantsDO{
// 		Version:      -1,
// 		Participants: newUsers,
// 	}
// 	elaC.AddUpsertData(channelID, &newChannlParticipant)

// 	meta, err := elaC.SelectMetaData(channelID)
// 	if err != nil {
// 		fmt.Println("LoadAllData Err: ", err)
// 	}
// 	fmt.Println("SelectMetaData: ", *meta)
// }

// Triển khai kịch bản xóa người dùng:
// - Xóa 1 người dùng
// - Xóa nhiều người
func DeleteUser() {}

// Triển khai kịch bản cập nhật:
// - Cập nhật thông tin của user mà không update version
// - Cập nhật thông tin của users cùng version
func UpdateUser() {
	fmt.Println("----------------------- Update Data ----------------------------")

	/*
		// Cập nhật 1 user - process: 1	time:  153 ms
		// Cập nhật 10 user - process: 1	time:  218 ms
		// Cập nhật 100 user - process: 1	time:  293 ms
		// Cập nhật 500 user - process: 1	time:  410 ms
		// Cập nhật 1000 user - process: 1	time:  573 ms
	*/

	newData, newDataID := sampleData(channelID, 1000, 500001)
	updateData, _ := sampleData(channelID, 1000, 1)

	// update với version tự động tăng
	println("Added new 1000 users")
	err := elaC.AddDataToCache(channelID, -1, newData)
	if err != nil {
		fmt.Println("AddDataToCache Err: ", err)
	}
	if ok := redisC.SaveAllData(channelID, newDataID); !ok {
		fmt.Println("SaveAllData Err!")
	}
	if err := redisC.SaveString(channelID, newDataID); err != nil {
		fmt.Println("SaveString Err!")
	}
	
	// update với version giữ nguyên
	println("Updated 1000 existing users")
	err = elaC.AddDataToCache(channelID+1, 10, updateData)
	if err != nil {
		fmt.Println("AddDataToCache Err: ", err)
		return
	}

	if ok := redisC.SaveAllData(channelID+1, newDataID); !ok {
		fmt.Println("SaveAllData Err!")
	}


}

// Triển khai kịch bản
func GetData() {
	fmt.Println("----------------------- Get Data ----------------------------")
	// timeStart := time.Now()

	_, _, err := elaC.GetUserAdmins(channelID, 10000, 0)
	if err != nil {
		fmt.Println("err get list: ", err)
	}

	_, ok := redisC.GetList(channelID)
	if !ok {
		fmt.Println("err get list!")
	}

	if _, err := redisC.GetString(channelID); err != nil {
		fmt.Println("err get list!")
	}
	// fmt.Println("Time to Get Data: ", time.Since(timeStart).Milliseconds(), "ms")
}
