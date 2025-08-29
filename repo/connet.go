package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/olivere/elastic/v7"
)

// ElasticMessagesDAO type
type ElasticChannelParticipantsDAO struct {
	client *elastic.Client
}

func GetChannelIndex(channelID int32) string {
	return fmt.Sprintf("channel_participant_%d", channelID)
}

func GetParicipantID(channelID int32, userID int64) string {
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

// Bump version cho channel:<id>:meta (version++), không đụng tới participant docs
func (e *ElasticChannelParticipantsDAO) SetChannelVersion(channelID int32, version int64) (int64, error) {
	if e == nil || e.client == nil {
		return 0, fmt.Errorf("DAO/client is nil")
	}
	if version <= 0 {
		return 0, fmt.Errorf("version must be > 0")
	}

	index := GetChannelIndex(channelID)
	if index == "" {
		return 0, fmt.Errorf("index is empty")
	}
	metaID := fmt.Sprintf("channel:%d:meta", channelID)
	now := time.Now().UTC()

	// Script: set cứng version = params.v
	scr := elastic.NewScript(`
		ctx._source.version = params.v;
		ctx._source.updated_at = params.ts;
	`).Param("v", version).Param("ts", now)

	// Nếu chưa có meta thì upsert với đúng version bạn truyền
	upsert := map[string]any{
		"channel_id": channelID,
		"version":    version,
		"updated_at": now,
	}

	resp, err := e.client.Update().
		Index(index).Id(metaID).
		Script(scr).Upsert(upsert).
		FetchSource(true).
		Do(context.Background())
	if err != nil {
		return 0, err
	}

	var src struct {
		Version int64 `json:"version"`
	}
	if resp.GetResult != nil && resp.GetResult.Source != nil {
		// LƯU Ý: cần deref *json.RawMessage
		_ = json.Unmarshal(resp.GetResult.Source, &src)
	}
	if src.Version == 0 {
		src.Version = version
	}
	return src.Version, nil
}

// SaveAllData: bulk Reset lại toàn bộ dữ liệu vào index theo channelID.
func (e *ElasticChannelParticipantsDAO) SaveAllDataBP(
	channelID int32,
	parts <-chan *ElasticUserChannleParticipantsDO,
) (string, error) {

	if e == nil || e.client == nil {
		return "", fmt.Errorf("DAO/client is nil")
	}
	indexName := GetChannelIndex(channelID)
	if indexName == "" {
		return "", fmt.Errorf("index is empty")
	}

	bp, err := e.client.BulkProcessor().
		Name(fmt.Sprintf("bp-channel-%d", channelID)).
		Workers(3).
		BulkActions(4000).
		BulkSize(15 << 20).
		FlushInterval(2 * time.Second).
		Backoff(elastic.NewExponentialBackoff(200*time.Millisecond, 5*time.Second)).
		After(func(execID int64, reqs []elastic.BulkableRequest, resp *elastic.BulkResponse, err error) {
			if err != nil {
				log.Printf("bulk batch error: %v", err)
				return
			}
			if resp != nil && resp.Errors {
				for _, item := range resp.Items {
					for _, r := range item {
						if r.Error != nil {
							log.Printf("bulk item failed: id=%s reason=%s", r.Id, r.Error.Reason)
						}
					}
				}
			}
		}).
		Do(context.Background())
	if err != nil {
		return "", err
	}
	defer bp.Close()

	for p := range parts {
		if p == nil {
			continue
		}
		id := GetParicipantID(channelID, p.UserID)

		req := elastic.NewBulkUpdateRequest().
			Index(indexName).
			Id(id).
			Routing(strconv.Itoa(int(channelID))). // base 10
			Doc(p).                                // partial doc để update
			DocAsUpsert(true)                      // nếu chưa có -> insert p

		bp.Add(req)
	}

	// 2) Upsert META (channel:<cid>:meta) với version
	metaID := GetChannelMeta(channelID)
	metaDoc := ElasticMetaChannelParticipantDO{
		ChannelID: channelID,
		Version: int32(0),
		UpdatedAt: time.Now().UTC(),
	}
	metaReq := elastic.NewBulkUpdateRequest().
		Index(indexName).
		Id(metaID).
		Routing(strconv.Itoa(int(channelID))).
		Doc(metaDoc).
		DocAsUpsert(true).
		RetryOnConflict(3)
	bp.Add(metaReq)

	if err := bp.Flush(); err != nil {
		return "", err
	}

	// đảm bảo tài liệu hiển thị ngay cho search
	if _, err := e.client.Refresh(indexName).Do(context.Background()); err != nil {
		return "", fmt.Errorf("refresh failed: %w", err)
	}

	return "ok", nil
}

// SaveAllData: bulk upsert toàn bộ dữ liệu vào index theo channelID.
// - Có tuỳ chọn refresh=wait_for (bật bằng biến cục bộ ngay dưới)
func (e *ElasticChannelParticipantsDAO) AddOrUpsertData(channelID int32, data *ElasticChannleParticipantsDO) string {
	if e == nil || e.client == nil {
		return "error: DAO/client is nil"
	}
	indexName := GetChannelIndex(channelID)
	if indexName == "" {
		return "error: index is empty"
	}
	if data == nil {
		return "error: data is nil"
	}

	const (
		requestTimeout = 30 * time.Second
		useRefreshWait = true // đặt false nếu không cần chờ index xong mới thấy khi search
	)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	route := strconv.Itoa(int(channelID))
	nowUTC := time.Now().UTC()

	bulk := e.client.Bulk()

	// 1) Nếu có version != 0 -> cập nhật meta/version của channel
	//    Lưu 1 doc "meta" cho channel để theo dõi version (id gợi ý)
	if data.Version != 0 {
		metaID := fmt.Sprintf("channel:%d:meta", channelID)
		metaDoc := ElasticMetaChannelParticipantDO{
			ChannelID: channelID,
			Version:   data.Version,
			UpdatedAt: nowUTC,
		}
		metaReq := elastic.NewBulkUpdateRequest().
			Index(indexName).
			Id(metaID).
			Routing(route).
			Doc(metaDoc).
			DocAsUpsert(true) // chưa có -> tạo mới meta
		bulk.Add(metaReq)
	}

	// 2) Upsert toàn bộ participants
	if len(data.Participants) > 0 {
		for _, p := range data.Participants {
			// Nếu struct p có ChannelID/UserID đầy đủ:
			pid := GetParicipantID(p.ChannelID, p.UserID) // ví dụ: "channel:<cid>:<uid>"

			req := elastic.NewBulkUpdateRequest().
				Index(indexName).
				Id(pid).
				Routing(route).
				Doc(p).            // partial doc
				DocAsUpsert(true). // chưa có -> insert p
				RetryOnConflict(3) // giảm xung đột phiên bản
			bulk.Add(req)
		}
	}

	if bulk.NumberOfActions() == 0 {
		return "ok: no-ops"
	}

	// Tuỳ chọn: chờ index xong để search thấy ngay
	if useRefreshWait {
		bulk = bulk.Refresh("wait_for")
	}

	resp, err := bulk.Do(ctx)
	if err != nil {
		return fmt.Sprintf("bulk error: %v", err)
	}
	if resp == nil {
		return "bulk error: nil response"
	}

	// Log lỗi từng item nếu có
	if resp.Errors {
		fail := 0
		for _, it := range resp.Items {
			for op, r := range it {
				if r.Error != nil {
					fail++
					log.Printf("bulk %s failed: id=%s status=%d reason=%s", op, r.Id, r.Status, r.Error.Reason)
				}
			}
		}
		return fmt.Sprintf("ok with errors: items=%d failed=%d", len(resp.Items), fail)
	}

	return fmt.Sprintf("ok: items=%d", len(resp.Items))
}

func (e *ElasticChannelParticipantsDAO) SelectListAdmins(ctx context.Context, channelID int32, limit, offset int) (
	*ElasticChannleParticipantsDO, int64, error,
) {
	if e == nil || e.client == nil {
		return nil, 0, fmt.Errorf("DAO/client is nil")
	}
	if limit <= 0 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	indexName := GetChannelIndex(channelID)

	// WHERE channel_id = ? AND is_left = 0 AND is_kicked = 0
	//   AND (is_creator = 1 OR admin_rights > 0)
	boolQ := elastic.NewBoolQuery().
		Filter(
			elastic.NewTermsQuery("channel_id", 1001),
			elastic.NewTermQuery("is_left", 0),
			elastic.NewTermQuery("is_kicked", 0),
		).
		Should(
			elastic.NewTermQuery("is_creator", 1),
			elastic.NewRangeQuery("admin_rights").Gt(0),
		).
		MinimumShouldMatch("1")

	search := e.client.Search().
		Index(indexName).
		Query(boolQ).
		From(offset).
		Size(limit).
		TrackTotalHits(true).
		Sort("user_id", true). // hoặc "promoted_at"/"joined_at" nếu có
		Routing(strconv.FormatInt(int64(channelID), 10))

	res, err := search.Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	total := int64(0)
	if res.Hits != nil && res.Hits.TotalHits != nil {
		total = res.Hits.TotalHits.Value
	}

	items := make([]ElasticUserChannleParticipantsDO, 0, len(res.Hits.Hits))
	for _, h := range res.Hits.Hits {
		var doc ElasticUserChannleParticipantsDO
		if err := json.Unmarshal(h.Source, &doc); err != nil {
			// nếu cần, log và skip 1 bản ghi lỗi
			fmt.Println("Unmarshal data err: ", err)

			continue
		}
		items = append(items, doc)
	}

	ChannlAdmin := ElasticChannleParticipantsDO{
		Version:      1,
		Participants: items,
	}

	return &ChannlAdmin, total, nil
}

func (e *ElasticChannelParticipantsDAO) DeleteParticipant(channelID int32, userIDs []int32) bool {
	if e == nil || e.client == nil {
		log.Printf("elastic client is nil")
		return false
	}
	indexName := GetChannelIndex(channelID)
	if indexName == "" {
		log.Printf("index is empty")
		return false
	}
	if len(userIDs) == 0 {
		return true // không có gì để xoá
	}

	ctx := context.Background()
	const chunkSize = 2000
	routing := strconv.Itoa(int(channelID))

	for start := 0; start < len(userIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(userIDs) {
			end = len(userIDs)
		}

		bulk := e.client.Bulk()
		for _, uid := range userIDs[start:end] {
			id := fmt.Sprintf("channel:%d:%d", channelID, uid)
			req := elastic.NewBulkDeleteRequest().
				Index(indexName).
				Id(id).
				Routing(routing)
			bulk.Add(req)
		}

		if bulk.NumberOfActions() == 0 {
			continue
		}

		resp, err := bulk.Do(ctx)
		if err != nil {
			log.Printf("bulk delete error: %v", err)
			return false
		}
		if resp == nil {
			log.Printf("bulk delete nil response")
			return false
		}

		// Log chi tiết lỗi từng item (nếu có)
		if resp.Errors {
			for _, item := range resp.Items {
				if d, ok := item["delete"]; ok {
					// 404 coi như đã xoá trước đó -> không fail
					if d.Error != nil && d.Status != 404 {
						log.Printf("delete failed: id=%s status=%d reason=%s", d.Id, d.Status, d.Error.Reason)
					}
				}
			}
		}
	}

	// Đảm bảo thấy ngay trên search
	if _, err := e.client.Refresh(indexName).Do(ctx); err != nil {
		log.Printf("refresh failed: %v", err)
		// không coi là fail nghiêm trọng
	}

	return true
}

func (e *ElasticChannelParticipantsDAO) SelectMetaData(channelID int32) (*ElasticMetaChannelParticipantDO, error) {
	if e == nil || e.client == nil {
		return nil, fmt.Errorf("DAO/client is nil")
	}
	indexName := GetChannelIndex(channelID)
	if indexName == "" {
		return nil, fmt.Errorf("index is empty")
	}

	idMeta := GetChannelMeta(channelID)   // ví dụ: "channel:<cid>:_meta"
	route := strconv.Itoa(int(channelID)) // base 10
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := e.client.Get().
		Index(indexName).
		Id(idMeta).
		Routing(route).
		Do(ctx)
	if elastic.IsNotFound(err) {
		fmt.Println("MetaData not found!")
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if resp == nil || !resp.Found {
		return nil, nil
	}

	var meta ElasticMetaChannelParticipantDO
	if err := json.Unmarshal(resp.Source, &meta); err != nil {
		// fallback: phòng trường hợp updated_at không parse được về time.Time
		var raw map[string]interface{}
		if err2 := json.Unmarshal(resp.Source, &raw); err2 != nil {
			return nil, fmt.Errorf("unmarshal meta failed: %v / %v", err, err2)
		}
		m := ElasticMetaChannelParticipantDO{}
		if v, ok := raw["channel_id"].(float64); ok {
			m.ChannelID = int32(v)
		}
		if v, ok := raw["version"].(float64); ok {
			m.Version = int32(v)
		}
		if v, ok := raw["updated_at"].(string); ok {
			if t, perr := time.Parse(time.RFC3339, v); perr == nil {
				m.UpdatedAt = t
			}
		}
		meta = m
	}

	return &meta, nil
}

// ---------------------------------------------------------------------------------------------
func ConnectElastic() *elastic.Client {
	// Thay đổi URL và thông tin đăng nhập cho phù hợp
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
