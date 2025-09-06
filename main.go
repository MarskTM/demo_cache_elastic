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
	// GetData()

	// Cập nhật dữ liệu
	// UpdateUser()

	// Xoá dữ liệu
	DeleteUser()
}

// ===================================================================================
// sampleData tạo ra 10 documents mẫu
func sampleData(channelID int32, size int, idStart int) ([]repo.ElasticChannelParticipantsDO, []int32) {
	now := int32(time.Now().Unix())

	data := []repo.ElasticChannelParticipantsDO{}
	litsUserID := []int32{}

	for i := idStart; i <= idStart+size; i++ {
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

func DeleteUser() {
	fmt.Println("----------------------- Delete Data ----------------------------")
	/*
		// Xoá 5K user - time:  489.2807ms
		// Xoá 10K user - time:  780.8454ms
		// Xoá 20K user - time:  2.058794s
		// Xoá 50K user - time:  3.4990352s
		// Xoá 100K user - time:  6.3435948s
	*/
	_, deleteDataID := sampleData(channelID, 100000, 95001)
	println("Deleted 1000 users")
	err := elaC.DeleteUsers(channelID+3, -1, deleteDataID)
	if err != nil {
		fmt.Println("DeleteUsers Err: ", err)
		return
	}

	if ok := redisC.DeleteUsers(channelID, deleteDataID); !ok {
		fmt.Println("DeleteUsers Err!")
	}
	if err := redisC.DeleteString(channelID, deleteDataID); err != nil {
		fmt.Println("DeleteUsersString Err!")
	}
}

// Triển khai kịch bản cập nhật:
// - Cập nhật thông tin của user mà không update version
// - Cập nhật thông tin của users cùng version
func UpdateUser() {
	fmt.Println("----------------------- Update Data ----------------------------")
	/*
		add:
			- 30K user	time: 3.2872372s - redisADD: 73.5411ms - redisString: 125.8157ms
			- 20K user	time: 3.0698894s - redisADD: 54.4387ms - redisString: 175.422ms
			- 10K user	time: 1.7960487s - redisADD: 48.6801ms - redisString: 113.255ms
		update:
			- 20K user	time: 3.1086109s - redisADD: 53.742ms - redisString: 134.6658ms
			- 20K user	time: 3.0211374s - redisADD: 65.094ms - redisString: 132.5555ms
			- 10K user	time: 1.9655146s - redisADD: 53.9629ms - redisString: 124.6259ms
		reload:
			- 60K user	time: 2.4188929s - redisADD: 81.9411ms - redisString: 4.9553ms
			- 50K user	time: 2.6445956s - redisADD: 73.9882ms - redisString: 4.9268ms
			- 40K user	time: 2.6665839s - redisADD: 61.8861ms - redisString: 4.0308ms
	*/

	newData, newDataID := sampleData(channelID, 30000, 500001)
	updateData, newUpdateID := sampleData(channelID, 30000, 1)

	// Thêm với version tự động tăng
	println("Added new 1000 users")
	err := elaC.AddDataToCache(channelID, -1, newData)
	if err != nil {
		fmt.Println("AddDataToCache Err: ", err)
	}
	if ok := redisC.AddUsers(channelID, newDataID); !ok {
		fmt.Println("AddUsers Err!")
	}
	if err := redisC.AddUsersString(channelID, newDataID); err != nil {
		fmt.Println("AddUsersString Err!")
	}

	// update với version
	println("Updated 1000 existing users")
	err = elaC.AddDataToCache(channelID+1, 10, updateData)
	if err != nil {
		fmt.Println("AddDataToCache Err: ", err)
		return
	}

	if ok := redisC.AddUsers(channelID+1, newUpdateID); !ok {
		fmt.Println("AddUsers Err!")
	}
	if err := redisC.AddUsersString(channelID+1, newUpdateID); err != nil {
		fmt.Println("AddUsersString Err!")
	}

	// update với dữ liệu mới (reset lại toàn bảng)
	println("reset 1000 existing users")
	reloadData, reloadDataID := sampleData(channelID+2, 60000, 32001)
	err = elaC.SaveAllUsers(channelID+3, -1, reloadData)
	if err != nil {
		fmt.Println("SaveAllUsers Err: ", err)
		return
	}
	if ok := redisC.SaveAllData(channelID+3, reloadDataID); !ok {
		fmt.Println("SaveAllData Err!")
	}
	if err := redisC.SaveString(channelID+3, reloadDataID); err != nil {
		fmt.Println("SaveString Err!")
	}
}

// Triển khai kịch bản
func GetData() {
	fmt.Println("----------------------- Get Data ----------------------------")
	// timeStart := time.Now()

	/*
		30K user - time:  1.7023381s - redisGetList: 128.2418ms - redisGetString: 34.4374ms
		20K user - time:  1.1935404s - redisGetList: 143.577ms - redisGetString: 31.2439ms
		10K user - time:  502.5181ms - redisGetList: 125.0407ms - redisGetString: 27.7333ms
	*/

	_, _, err := elaC.GetUserAdmins(channelID, 30000, 0)
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
