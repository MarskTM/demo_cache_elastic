package repo

import "time"

// ElasticChatParticipantsDO type;
type ElasticChannleParticipantsDO struct {
	Version      int32 `json:"version"`
	Participants []ElasticUserChannleParticipantsDO
}

// ------------------------------- Elastic Model -----------------------------------------
type ElasticUserChannleParticipantsDO struct {
	ID                int64                  `json:"id"`
	ChannelID         int32                  `json:"channel_id"`
	UserID            int64                  `json:"user_id,omitempty"`
	IsCreator         int32                  `json:"is_creator"`
	AdminRights       int32                  `json:"admin_rights"`
	ParticipantType   int8                   `json:"participant_type"`
	HiddenParticipant int8                   `json:"hidden_participant"`
	IsLeft            int8                   `json:"is_left"`
	LeftAt            int32                  `json:"left_at"`
	IsKicked          int8                   `json:"is_kicked"`
	BannedRights      int32                  `json:"banned_rights"`
	BannedUntilDate   int32                  `json:"banned_until_date"`
	Data              *ChannelParticipantsDO `json:"data"`
}

type ChannelParticipantsDO struct {
	ID                        int64  `db:"id"`
	ChannelID                 int32  `db:"channel_id"`
	UserID                    int32  `db:"user_id"`
	IsCreator                 int32  `db:"is_creator"`
	ParticipantType           int8   `db:"participant_type"`
	InviterUserID             int32  `db:"inviter_user_id"`
	InvitedAt                 int32  `db:"invited_at"`
	JoinedAt                  int32  `db:"joined_at"`
	IsWaitingAprrove          int8   `db:"is_waiting_approve"`
	HiddenParticipant         int8   `db:"hidden_participant"`
	IsLeft                    int8   `db:"is_left"`
	LeftAt                    int32  `db:"left_at"`
	IsKicked                  int8   `db:"is_kicked"`
	KickedBy                  int32  `db:"kicked_by"`
	KickedAt                  int32  `db:"kicked_at"`
	HiddenPrehistory          int8   `db:"hidden_prehistory"`
	HiddenPrehistoryMessageID int32  `db:"hidden_prehistory_message_id"`
	AdminRights               int32  `db:"admin_rights"`
	PromotedBy                int32  `db:"promoted_by"`
	PromotedAt                int32  `db:"promoted_at"`
	Rank                      string `db:"rank"`
	BannedRights              int32  `db:"banned_rights"`
	BannedUntilDate           int32  `db:"banned_until_date"`
	BannedAt                  int32  `db:"banned_at"`
	ReadInboxMaxID            int32  `db:"read_inbox_max_id"`
	ReadOutboxMaxID           int32  `db:"read_outbox_max_id"`
	Date                      int32  `db:"date"`
	State                     int8   `db:"state"`
	CreatedAt                 string `db:"created_at"`
	UpdatedAt                 string `db:"updated_at"`
}

type ElasticMetaChannelParticipantDO struct {
	ChannelID int32     `json:"channel_id"`
	Version   int32     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}
