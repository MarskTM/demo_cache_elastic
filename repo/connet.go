package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
)

const (
	ELASTIC_SIZE_INDEX = 1000 // số shard index
)

// ElasticMessagesDAO type
type ElasticChannelParticipantsDAO struct {
	client *elastic.Client
}

func GetElasticChannelIndex(channelID int32, sizeIndex int32) string {
	if channelID <= 0 {
		return ""
	}

	shard := channelID % sizeIndex
	return fmt.Sprintf("channel_participants_%03d", shard)
}

func GetParicipantID(channelID int32, userID int32) string {
	return fmt.Sprintf("channel:%d:%d", channelID, userID)
}

func GetChannelMeta(channelID int32) string {
	return fmt.Sprintf("channel:%d:meta", channelID)
}

// -------------------------------------------------------------------------------------------
// NewElasticMessagesDAO func
func NewElasticChannelParticipantsDAO(client *elastic.Client) *ElasticChannelParticipantsDAO {
	return &ElasticChannelParticipantsDAO{client}
}

// SaveAllUsers reload lại toàn bộ data lên elastic.
// Đặt version = -1 nếu không muốn cập nhật version.
// Đặt version = 0 nếu không muốn cập nhật version.
func (e *ElasticChannelParticipantsDAO) SaveAllUsers(channelID int32, version int32, list []ElasticChannelParticipantsDO) error {
	timeStart := time.Now()
	if e == nil || e.client == nil {
		return fmt.Errorf("DAO/client is nil")
	}
	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return fmt.Errorf("index is empty")
	}
	ctx := context.Background()
	if err := e.ensureIndexExists(ctx, indexName); err != nil {
		return err
	}
	// fmt.Println("Start SaveAllUsers ...")

	route := strconv.Itoa(int(channelID))
	metaID := GetChannelMeta(channelID)

	// 1. Xóa data cũ của theo channelID
	q := elastic.NewBoolQuery().
		Filter(elastic.NewTermQuery("channel_id", channelID)).
		MustNot(elastic.NewIdsQuery().Ids(metaID))

	_, err := e.client.DeleteByQuery(indexName).
		Query(q).
		Conflicts("proceed"). // tiếp tục chạy nếu có xung đột version
		Routing(route).       // request đến đúng shard, không cần broadcast toàn cluster.
		// WaitForCompletion(false). // chạy bất đồng bộ, không đợi kết quả xong ngay lập tức.
		RequestsPerSecond(5000). // -1 không throttle, tốc độ xử lý docs/giây.
		Do(ctx)
	if err != nil && !elastic.IsNotFound(err) {
		return fmt.Errorf("delete participants failed: %w", err)
	}

	// 2. Tạo BulkProcessor
	bp, err := e.client.BulkProcessor().
		Name(fmt.Sprintf("bp-channel-%d", channelID)).
		Workers(3).                     // số goroutine xử lý bulk song song
		BulkActions(4000).              // tối đa 4000 req/batch
		BulkSize(15 << 20).             // tối đa 15MB/batch
		FlushInterval(1 * time.Second). // auto flush sau 1s nếu chưa đủ batch
		Backoff(elastic.NewExponentialBackoff(
			200*time.Millisecond, 1*time.Second, // retry từ 200ms đến 1s
		)). // retry backoff
		After(func(execID int64, reqs []elastic.BulkableRequest, resp *elastic.BulkResponse, err error) {
			if err != nil {
				// glog.V(1).Infof("bulk batch error: %v", err)
				fmt.Printf("bulk batch error: %v\n", err)
				return
			}
			// if resp != nil && resp.Errors {
			// 	for _, item := range resp.Items {
			// 		for _, r := range item {
			// 			if r.Error != nil {
			// 				glog.V(3).Infof("bulk item failed: id=%s reason=%s", r.Id, r.Error.Reason)
			// 			}
			// 		}
			// 	}
			// }
		}).
		Do(context.Background())
	if err != nil {
		return err
	}
	defer bp.Close()

	for _, p := range list {
		id := GetParicipantID(channelID, p.UserID)
		req := elastic.NewBulkIndexRequest().
			Index(indexName).
			Id(id).
			Routing(strconv.Itoa(int(channelID))). // định tuyến đúng shard
			Doc(p)

		bp.Add(req)
	}

	// 3. Flush và đợi hoàn tất
	if err := bp.Flush(); err != nil {
		return err
	}

	// 4. Upsert META (channel:<cid>:meta) với version nếu có
	if err := e.SetVersion(channelID, version); err != nil {
		return fmt.Errorf("set version failed: %w", err)
	}

	// fmt.Println("Bulk index completed.")
	duration := time.Since(timeStart)
	fmt.Printf("SaveAllUsers completed. Time: %s - total: %d\n", duration, len(list))
	return nil
}

// Hàm thực hiện để Add hoặc Update lại thông tin các participant được truyền vào.
// Nếu muốn cập nhật kích thước, số lượng participants khi có người rời nhóm -> dùng SaveAllUser hoặc DeleteUser.
// Đặt version = -1 nếu muốn tự động tăng version.
// Đặt version = 0 nếu không muốn cập nhật lại version.
func (e *ElasticChannelParticipantsDAO) AddDataToCache(channelID int32, version int32, list []ElasticChannelParticipantsDO) error {
	timeStart := time.Now()
	if e == nil || e.client == nil {
		return fmt.Errorf("DAO/client is nil")
	}
	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return fmt.Errorf("index is empty")
	}

	// 1. Tạo BulkProcessor
	bp, err := e.client.BulkProcessor().
		Name(fmt.Sprintf("bp-channel-add-%d", channelID)).
		Workers(3).                                                                  // số goroutine xử lý bulk song song
		BulkActions(4000).                                                           // tối đa 4000 req/batch
		BulkSize(15 << 20).                                                          // tối đa 15MB/batch
		FlushInterval(1 * time.Second).                                              // auto flush sau 1s nếu chưa đủ batch
		Backoff(elastic.NewExponentialBackoff(200*time.Millisecond, 1*time.Second)). // retry backoff
		After(func(execID int64, reqs []elastic.BulkableRequest, resp *elastic.BulkResponse, err error) {
			if err != nil {
				// glog.V(3).Info("bulk batch error: %v", err)
				fmt.Printf("bulk batch error: %v\n", err)
				return
			}
			// if resp != nil && resp.Errors {
			// 	for _, item := range resp.Items {
			// 		for _, r := range item {
			// 			if r.Error != nil {
			// 				glog.V(3).Info("bulk item failed: id=%s reason=%s", r.Id, r.Error.Reason)
			// 			}
			// 		}
			// 	}
			// }
		}).
		Do(context.Background())
	if err != nil {
		return err
	}
	defer bp.Close()

	for _, p := range list {
		id := GetParicipantID(channelID, p.UserID)

		req := elastic.NewBulkUpdateRequest().
			Index(indexName).
			Id(id).
			Routing(strconv.Itoa(int(channelID))). // base 10
			Doc(p).                                // partial doc để update
			DocAsUpsert(true)                      // nếu chưa có -> insert p

		bp.Add(req)
	}

	if err := bp.Flush(); err != nil {
		return err
	}

	if err := e.SetVersion(channelID, version); err != nil {
		return fmt.Errorf("set version after delete failed: %w", err)
	}

	// đảm bảo tài liệu hiển thị ngay cho search
	if _, err := e.client.Refresh(indexName).Do(context.Background()); err != nil {
		return fmt.Errorf("refresh failed: %w", err)
	}
	duration := time.Since(timeStart)
	fmt.Printf("AddDataToCache completed. Time: %s - total: %d\n", duration, len(list))
	return nil
}

// ------------------------------------------------------------------------------------------------------------------------
func (e *ElasticChannelParticipantsDAO) GetUserAdmins(channelID int32, limit, offset int32) ([]ChannelParticipantsDO, int32, error) {
	timeStart := time.Now()
	if e == nil || e.client == nil {
		return nil, 0, fmt.Errorf("DAO/client is nil")
	}
	if offset < 0 {
		offset = 0
	}

	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return nil, 0, fmt.Errorf("index is empty")
	}

	ctx := context.Background()
	route := strconv.FormatInt(int64(channelID), 10)

	// WHERE channel_id = ? AND is_left = 0 AND is_kicked = 0 AND hidden_participant = 0
	boolQ := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermQuery("channel_id", channelID),
			elastic.NewTermQuery("is_left", 0),
			elastic.NewTermQuery("is_kicked", 0),
			elastic.NewTermQuery("hidden_participant", 0),
		).
		Should(
			elastic.NewTermQuery("is_creator", 1),       // is_creator = 1
			elastic.NewRangeQuery("admin_rights").Gt(0), // admin_rights > 0
		).
		MinimumShouldMatch("1")

	// --------- Lấy hết (limit == -1): dùng Scroll ----------
	if limit == -1 || int(offset)+int(limit) > 10000 {
		const batch = 2000
		scroll := e.client.Scroll(indexName).
			Query(boolQ).
			Size(batch).
			Sort("user_id", false).
			Routing(route).
			Scroll("1m")
		defer scroll.Clear(ctx)

		items := make([]ChannelParticipantsDO, 0, min(int(limit), batch))
		want := int(limit)
		skipped := int(offset)
		var total int64

		for want > 0 {
			res, err := scroll.Do(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, 0, fmt.Errorf("scroll failed: %w", err)
			}
			if res == nil || res.Hits == nil || len(res.Hits.Hits) == 0 {
				break
			}

			total += int64(len(res.Hits.Hits))
			for _, h := range res.Hits.Hits {
				if skipped > 0 {
					skipped--
					continue
				}
				var doc ChannelParticipantsDO
				if err := json.Unmarshal(h.Source, &doc); err != nil {
					continue
				}
				items = append(items, doc)
				want--
				if want == 0 {
					break
				}
			}
		}
		duration := time.Since(timeStart)
		fmt.Printf("Thời gian thực thi của hàm GetUserAdmins (scroll): %s - total: %d \n", duration, total)
		return items, int32(total), nil
	}

	// --------- Phân trang bình thường (from/size) ----------
	res, err := e.client.Search().
		Index(indexName).
		Query(boolQ).
		From(int(offset)).
		Size(int(limit)).
		TrackTotalHits(true).
		Sort("user_id", false). // desc
		Routing(route).
		Do(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("search failed: %w", err)
	}

	var total int64
	if res.Hits != nil && res.Hits.TotalHits != nil {
		total = res.Hits.TotalHits.Value
	}

	items := make([]ChannelParticipantsDO, 0, len(res.Hits.Hits))
	for _, h := range res.Hits.Hits {
		var doc ChannelParticipantsDO
		if err := json.Unmarshal(h.Source, &doc); err != nil {
			fmt.Println("Unmarshal data err:", err)
			continue
		}
		items = append(items, doc)
	}
	duration := time.Since(timeStart)
	fmt.Printf("Thời gian thực thi của hàm GetUserAdmins: %s - total: %d \n", duration, total)
	return items, int32(total), nil
}

// ------------------------------------------------------------------------------------------------------------------------
// Lấy version hiện tại của channel
func (e *ElasticChannelParticipantsDAO) GetVersion(channelID int32) (*ElasticChannelParticipantMetaDO, error) {
	if e == nil || e.client == nil {
		return nil, fmt.Errorf("DAO/client is nil")
	}

	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return nil, fmt.Errorf("index is empty")
	}

	ctx := context.Background()
	route := strconv.Itoa(int(channelID)) // request đến đúng shard, tránh broadcast toàn cluster.
	metaID := GetChannelMeta(channelID)   // ví dụ: "channel:123:meta"

	// Parse source
	meta := ElasticChannelParticipantMetaDO{}

	// Lấy document meta
	resp, err := e.client.Get().
		Index(indexName).
		Id(metaID).
		Routing(route).
		Do(ctx)
	if err != nil {
		if elastic.IsNotFound(err) {
			// chưa có version → mặc định 0
			return &meta, nil
		}
		return nil, fmt.Errorf("get meta failed: %w", err)
	}
	if !resp.Found {
		return &meta, nil
	}

	if err := json.Unmarshal(resp.Source, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta failed: %w", err)
	}

	return &meta, nil
}

// Đặt version = -1 để tự động tăng.
// Đặt version = 0 để bỏ qua update version.
func (e *ElasticChannelParticipantsDAO) SetVersion(channelID int32, version int32) error {
	if e == nil || e.client == nil {
		return fmt.Errorf("DAO/client is nil")
	}
	if version == 0 {
		return nil
	}

	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return fmt.Errorf("index is empty")
	}

	ctx := context.Background()
	route := strconv.Itoa(int(channelID))
	metaID := GetChannelMeta(channelID)

	now := time.Now().UTC()

	if version == -1 {
		// Atomic increment: nếu chưa có doc → upsert version=1
		script := elastic.NewScript(`
			if (ctx._source.version == null) {
				ctx._source.version = params.start;
			} else {
				ctx._source.version += params.inc;
			}
			ctx._source.updated = params.now;
		`).
			Param("start", 1).
			Param("inc", 1).
			Param("now", now)

		_, err := e.client.Update().
			Index(indexName).
			Id(metaID).
			Routing(route).
			Script(script).
			// scripted upsert bảo đảm chạy script cả khi doc chưa tồn tại
			ScriptedUpsert(true).
			// Upsert body chỉ cần tối thiểu để init; script sẽ set lại updated
			Upsert(ElasticChannelParticipantMetaDO{
				ChannelID: channelID,
				Version:   1,
				UpdateAt:  now.Unix(),
			}).
			Refresh("wait_for").
			RetryOnConflict(3).
			Do(ctx)
		if err != nil {
			return fmt.Errorf("update (increment) failed: %w", err)
		}
		return nil
	}

	meta := ElasticChannelParticipantMetaDO{
		ChannelID: channelID,
		Version:   version,
		UpdateAt:  now.Unix(),
	}

	_, err := e.client.Update().
		Index(indexName).
		Id(metaID).
		Routing(route).
		Doc(meta).         // partial update các field trong struct
		DocAsUpsert(true). // nếu chưa có thì upsert
		Refresh("wait_for").
		RetryOnConflict(3).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("update meta failed: %w", err)
	}

	return nil
}

// Đặt version = -1 để tự động tăng.
// Đặt version = 0 để bỏ qua.
func (e *ElasticChannelParticipantsDAO) DeleteUsers(channelID int32, version int32, listUserID []int32) error {
	timeStart := time.Now()
	if e == nil || e.client == nil {
		return fmt.Errorf("DAO/client is nil")
	}
	indexName := GetElasticChannelIndex(channelID, ELASTIC_SIZE_INDEX)
	if indexName == "" {
		return fmt.Errorf("index is empty")
	}
	if len(listUserID) == 0 {
		return fmt.Errorf("listUserID empty")
	}

	ctx := context.Background()
	route := strconv.Itoa(int(channelID))

	const chunkSize = 1000
	for i := 0; i < len(listUserID); i += chunkSize {
		end := i + chunkSize
		if end > len(listUserID) {
			end = len(listUserID)
		}
		part := listUserID[i:end]

		terms := make([]interface{}, len(part))
		for j, uid := range part {
			terms[j] = uid
		}

		// Nên filter thêm channel_id để an toàn
		q := elastic.NewBoolQuery().
			Filter(
				elastic.NewTermQuery("channel_id", channelID),
				elastic.NewTermsQuery("user_id", terms...),
			)

		resp, err := e.client.DeleteByQuery(indexName).
			Query(q).
			Routing(route).
			Conflicts("proceed").
			// Refresh chỉ nhận true/false cho delete_by_query
			Refresh("true"). // hoặc .Refresh("false") / bỏ hẳn
			WaitForCompletion(true).
			// (tùy chọn) tăng tốc bằng slicing:
			// Slices(4).
			Do(ctx)
		if err != nil {
			return fmt.Errorf("delete by query failed: %w", err)
		}
		if resp == nil || len(resp.Failures) > 0 {
			return fmt.Errorf("delete by query has %d failures", len(resp.Failures))
		}
	}

	// cập nhật meta version (hỗ trợ version = -1 để auto-increment)
	if err := e.SetVersion(channelID, version); err != nil {
		return fmt.Errorf("set version after delete failed: %w", err)
	}
	fmt.Printf("DeleteUsers completed. Time: %s\n", time.Since(timeStart))
	return nil
}

// ---------------------------------------------------------------------------------------------
func ConnectElastic() *elastic.Client {
	// Thay đổi URL và thông tin đăng nhập cho phù hợp
	// esURL := "http://10.8.14.55:9200"
	esURL := "http://localhost:9200"
	username := "elastic"     // nếu bạn tắt security, để ""
	password := "changeme123" // nếu bạn tắt security, để ""

	client, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false), // disable sniff khi chạy local / docker
		elastic.SetBasicAuth(username, password),
	)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// Kiểm tra kết nối
	info, code, err := client.Ping(esURL).Do(context.Background())
	if err != nil {
		log.Fatalf("Error pinging ES: %s", err)
	}
	fmt.Printf("✅ Kết nối Elasticsearch thành công - code %d and version %s\n", code, info.Version.Number)

	return client
}

func (e *ElasticChannelParticipantsDAO) ensureIndexExists(ctx context.Context, indexName string) error {
	exists, err := e.client.IndexExists(indexName).Do(ctx)
	if err != nil {
		return fmt.Errorf("check index exists failed: %w", err)
	}
	if exists {
		return nil
	}

	// Mapping gợi ý – chỉnh theo schema của bạn
	body := map[string]any{
		// "settings": map[string]any{
		// 	"number_of_shards":   1,
		// 	"number_of_replicas": 0,
		// 	// Có thể tạm tắt refresh khi bulk lớn, rồi set lại sau:
		// 	// "refresh_interval": "-1",
		// },
		// "mappings": map[string]any{
		// 	"properties": map[string]any{
		// 		"channel_id":   map[string]any{"type": "integer"},
		// 		"user_id":      map[string]any{"type": "integer"},
		// 		"version":      map[string]any{"type": "integer"},
		// 		"update_at":    map[string]any{"type": "date", "format": "epoch_second"},
		// 		"is_left":      map[string]any{"type": "integer"},
		// 		"is_kicked":    map[string]any{"type": "integer"},
		// 		"is_creator":   map[string]any{"type": "integer"},
		// 		"admin_rights": map[string]any{"type": "integer"},
		// 		"is_meta":      map[string]any{"type": "boolean"},
		// 	},
		// },
	}

	resp, err := e.client.CreateIndex(indexName).BodyJson(body).Do(ctx)
	if err != nil {
		// Bỏ qua nếu 2 tiến trình cùng lúc tạo => "resource_already_exists_exception"
		if strings.Contains(err.Error(), "resource_already_exists_exception") {
			return nil
		}
		return fmt.Errorf("create index %s failed: %w", indexName, err)
	}
	if !resp.Acknowledged {
		return fmt.Errorf("create index %s not acknowledged", indexName)
	}
	return nil
}
