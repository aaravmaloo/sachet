package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type StatsStore struct {
	mu   sync.RWMutex
	path string
	data PersistentData
}

type PersistentData struct {
	Groups map[string]*GroupStats `json:"groups"`
}

type GroupRules struct {
	OnlyAdminsCanMessage  bool   `json:"only_admins_can_message"`
	OnlyAdminsCanEditInfo bool   `json:"only_admins_can_edit_info"`
	JoinApprovalRequired  bool   `json:"join_approval_required"`
	DisappearingTimerSec  uint32 `json:"disappearing_timer_sec"`
}

type GroupStats struct {
	JID              string                  `json:"jid"`
	Name             string                  `json:"name"`
	ScanEnabled      bool                    `json:"scan_enabled"`
	LastScannedAt    time.Time               `json:"last_scanned_at"`
	TotalMessages    int64                   `json:"total_messages"`
	MessagesPerDay   map[string]int64        `json:"messages_per_day"`
	Members          map[string]*MemberStats `json:"members"`
	Rules            GroupRules              `json:"rules"`
	AdminCount       int                     `json:"admin_count"`
	SuperAdminCount  int                     `json:"super_admin_count"`
	ParticipantCount int                     `json:"participant_count"`
}

type MemberStats struct {
	JID            string           `json:"jid"`
	Name           string           `json:"name"`
	TotalMessages  int64            `json:"total_messages"`
	MessagesPerDay map[string]int64 `json:"messages_per_day"`
	FirstSeen      time.Time        `json:"first_seen"`
	LastSeen       time.Time        `json:"last_seen"`
}

type LeaderboardEntry struct {
	JID   string
	Name  string
	Count int64
}

func NewStatsStore(path string) (*StatsStore, error) {
	store := &StatsStore{
		path: path,
		data: PersistentData{
			Groups: make(map[string]*GroupStats),
		},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *StatsStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var parsed PersistentData
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return err
	}
	if parsed.Groups == nil {
		parsed.Groups = make(map[string]*GroupStats)
	}
	s.data = parsed
	return nil
}

func (s *StatsStore) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil && filepath.Dir(s.path) != "." {
		return err
	}

	buf, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *StatsStore) GetOrCreateGroup(groupJID string) *GroupStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.getOrCreateGroupLocked(groupJID)
}

func (s *StatsStore) getOrCreateGroupLocked(groupJID string) *GroupStats {
	if group, ok := s.data.Groups[groupJID]; ok {
		if group.MessagesPerDay == nil {
			group.MessagesPerDay = make(map[string]int64)
		}
		if group.Members == nil {
			group.Members = make(map[string]*MemberStats)
		}
		return group
	}

	group := &GroupStats{
		JID:            groupJID,
		MessagesPerDay: make(map[string]int64),
		Members:        make(map[string]*MemberStats),
	}
	s.data.Groups[groupJID] = group
	return group
}

func (s *StatsStore) ResetGroup(groupJID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.getOrCreateGroupLocked(groupJID)
	name := old.Name
	rules := old.Rules
	adminCount := old.AdminCount
	superAdminCount := old.SuperAdminCount
	participantCount := old.ParticipantCount
	scanEnabled := old.ScanEnabled

	s.data.Groups[groupJID] = &GroupStats{
		JID:              groupJID,
		Name:             name,
		ScanEnabled:      scanEnabled,
		MessagesPerDay:   make(map[string]int64),
		Members:          make(map[string]*MemberStats),
		Rules:            rules,
		AdminCount:       adminCount,
		SuperAdminCount:  superAdminCount,
		ParticipantCount: participantCount,
	}
	return s.saveLocked()
}

func (s *StatsStore) UpdateGroupMetadata(groupJID, groupName string, rules GroupRules, adminCount, superAdminCount, participantCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := s.getOrCreateGroupLocked(groupJID)
	group.Name = groupName
	group.Rules = rules
	group.AdminCount = adminCount
	group.SuperAdminCount = superAdminCount
	group.ParticipantCount = participantCount
	return s.saveLocked()
}

func (s *StatsStore) SetScanEnabled(groupJID string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := s.getOrCreateGroupLocked(groupJID)
	group.ScanEnabled = enabled
	if enabled {
		group.LastScannedAt = time.Now().UTC()
	}
	return s.saveLocked()
}

func (s *StatsStore) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *StatsStore) IsScanEnabled(groupJID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[groupJID]
	return ok && group.ScanEnabled
}

func (s *StatsStore) RecordMessage(groupJID, groupName, memberJID, memberName string, ts time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group := s.getOrCreateGroupLocked(groupJID)
	if groupName != "" {
		group.Name = groupName
	}

	dayKey := ts.UTC().Format("2006-01-02")
	group.TotalMessages++
	group.MessagesPerDay[dayKey]++

	member, ok := group.Members[memberJID]
	if !ok {
		member = &MemberStats{
			JID:            memberJID,
			MessagesPerDay: make(map[string]int64),
			FirstSeen:      ts.UTC(),
		}
		group.Members[memberJID] = member
	}
	if memberName != "" {
		member.Name = memberName
	}
	member.TotalMessages++
	member.MessagesPerDay[dayKey]++
	if member.FirstSeen.IsZero() || ts.UTC().Before(member.FirstSeen) {
		member.FirstSeen = ts.UTC()
	}
	if ts.UTC().After(member.LastSeen) {
		member.LastSeen = ts.UTC()
	}

	return nil
}

func (s *StatsStore) SnapshotGroup(groupJID string) (*GroupStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[groupJID]
	if !ok {
		return nil, false
	}
	out := *group
	out.MessagesPerDay = cloneIntMap(group.MessagesPerDay)
	out.Members = make(map[string]*MemberStats, len(group.Members))
	for jid, member := range group.Members {
		memberCopy := *member
		memberCopy.MessagesPerDay = cloneIntMap(member.MessagesPerDay)
		out.Members[jid] = &memberCopy
	}
	return &out, true
}

func (s *StatsStore) ResolveMember(groupJID, rawArg, fallbackSenderJID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[groupJID]
	if !ok || len(group.Members) == 0 {
		return fallbackSenderJID
	}

	arg := strings.TrimSpace(strings.TrimPrefix(rawArg, "@"))
	if arg == "" {
		return fallbackSenderJID
	}

	if _, exists := group.Members[arg]; exists {
		return arg
	}

	digits := onlyDigits(arg)
	if digits != "" {
		for jid := range group.Members {
			if strings.HasPrefix(jid, digits+"@") || strings.Contains(jid, digits) {
				return jid
			}
		}
	}

	lower := strings.ToLower(arg)
	for jid, member := range group.Members {
		if strings.Contains(strings.ToLower(member.Name), lower) {
			return jid
		}
	}

	return fallbackSenderJID
}

func (s *StatsStore) Leaderboard(groupJID string, limit int) []LeaderboardEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[groupJID]
	if !ok {
		return nil
	}
	rows := make([]LeaderboardEntry, 0, len(group.Members))
	for jid, member := range group.Members {
		rows = append(rows, LeaderboardEntry{
			JID:   jid,
			Name:  member.Name,
			Count: member.TotalMessages,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count == rows[j].Count {
			return rows[i].JID < rows[j].JID
		}
		return rows[i].Count > rows[j].Count
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func cloneIntMap(in map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func onlyDigits(in string) string {
	var b strings.Builder
	for _, ch := range in {
		if ch >= '0' && ch <= '9' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}
