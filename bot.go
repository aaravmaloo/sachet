package main

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type SachetBot struct {
	client *whatsmeow.Client
	store  *StatsStore
	cfg    Config
	log    waLog.Logger

	saveMu    sync.Mutex
	dirty     bool
	lastSaved time.Time

	scanMu      sync.Mutex
	activeScans map[string]*scanSession
}

type scanSession struct {
	resultCh chan scanBatchResult
}

type scanBatchResult struct {
	Processed int
	Oldest    *types.MessageInfo
	EndOfChat bool
}

func NewSachetBot(client *whatsmeow.Client, store *StatsStore, cfg Config, log waLog.Logger) *SachetBot {
	return &SachetBot{
		client:      client,
		store:       store,
		cfg:         cfg,
		log:         log,
		activeScans: make(map[string]*scanSession),
	}
}

func (b *SachetBot) HandleEvent(raw interface{}) {
	switch evt := raw.(type) {
	case *events.Message:
		b.handleIncomingMessage(evt)
	case *events.HistorySync:
		b.handleHistorySync(evt)
	case *events.Connected:
		b.log.Infof("Connected to WhatsApp")
	case *events.Disconnected:
		b.log.Warnf("Disconnected from WhatsApp")
	}
}

func (b *SachetBot) handleIncomingMessage(evt *events.Message) {
	if evt == nil || evt.Message == nil || !evt.Info.IsGroup {
		return
	}


	msgText := extractMessageText(evt.Message)
	trimmed := strings.TrimSpace(msgText)
	if strings.HasPrefix(trimmed, b.cfg.CommandPrefix) {
		b.handleCommand(evt, trimmed)
	}

	if b.store.IsScanEnabled(evt.Info.Chat.String()) && isCountableMessage(evt.Message) {
		_ = b.store.RecordMessage(
			evt.Info.Chat.String(),
			"",
			evt.Info.Sender.String(),
			evt.Info.PushName,
			evt.Info.Timestamp,
		)
		b.markDirty()
		b.maybeFlush(false)
	}
}

func (b *SachetBot) handleHistorySync(evt *events.HistorySync) {
	if evt == nil || evt.Data == nil {
		return
	}

	type aggregate struct {
		processed int
		oldest    *types.MessageInfo
		endOfChat bool
	}
	results := make(map[string]*aggregate)

	for _, conv := range evt.Data.GetConversations() {
		chatJID, err := types.ParseJID(conv.GetID())
		if err != nil || chatJID.Server != types.GroupServer {
			continue
		}

		r := b.processHistoryConversation(chatJID, conv.GetMessages())
		key := chatJID.String()
		existing, ok := results[key]
		if !ok {
			results[key] = &aggregate{
				processed: r.Processed,
				oldest:    r.Oldest,
				endOfChat: conv.GetEndOfHistoryTransfer(),
			}
			continue
		}
		existing.processed += r.Processed
		existing.endOfChat = existing.endOfChat || conv.GetEndOfHistoryTransfer()
		if existing.oldest == nil || (r.Oldest != nil && r.Oldest.Timestamp.Before(existing.oldest.Timestamp)) {
			existing.oldest = r.Oldest
		}
	}

	if len(results) > 0 {
		b.markDirty()
		b.maybeFlush(true)
	}

	for chat, r := range results {
		b.notifyScanResult(chat, scanBatchResult{
			Processed: r.processed,
			Oldest:    r.oldest,
			EndOfChat: r.endOfChat,
		})
	}
}

func (b *SachetBot) processHistoryConversation(chatJID types.JID, messages []*waHistorySync.HistorySyncMsg) scanBatchResult {
	if len(messages) == 0 {
		return scanBatchResult{}
	}

	type parsedMessage struct {
		info *types.MessageInfo
	}

	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan *waHistorySync.HistorySyncMsg)
	out := make(chan parsedMessage, len(messages))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range jobs {
				webMsg := msg.GetMessage()
				if webMsg == nil {
					continue
				}
				parsed, err := b.client.ParseWebMessage(chatJID, webMsg)
				if err != nil || parsed == nil || parsed.Message == nil {
					continue
				}
				if !isCountableMessage(parsed.Message) {
					continue
				}
				if !b.store.IsScanEnabled(parsed.Info.Chat.String()) {
					continue
				}
				_ = b.store.RecordMessage(
					parsed.Info.Chat.String(),
					"",
					parsed.Info.Sender.String(),
					parsed.Info.PushName,
					parsed.Info.Timestamp,
				)
				infoCopy := parsed.Info
				out <- parsedMessage{info: &infoCopy}
			}
		}()
	}

	go func() {
		for _, msg := range messages {
			jobs <- msg
		}
		close(jobs)
		wg.Wait()
		close(out)
	}()

	var oldest *types.MessageInfo
	processed := 0
	for parsed := range out {
		processed++
		if oldest == nil || parsed.info.Timestamp.Before(oldest.Timestamp) {
			oldest = parsed.info
		}
	}

	return scanBatchResult{
		Processed: processed,
		Oldest:    oldest,
	}
}

func (b *SachetBot) handleCommand(evt *events.Message, cmdText string) {
	fields := strings.Fields(cmdText)
	if len(fields) == 0 {
		return
	}
	command := strings.ToLower(fields[0])

	switch command {
	case b.cfg.CommandPrefix + "scan":
		b.handleScanCommand(evt)
	case b.cfg.CommandPrefix + "list":
		b.handleListCommand(evt)
	case b.cfg.CommandPrefix + "stats":
		arg := ""
		if len(fields) > 1 {
			arg = strings.Join(fields[1:], " ")
		}
		b.handleStatsCommand(evt, arg)
	case b.cfg.CommandPrefix + "leaderboard":
		b.handleLeaderboardCommand(evt)
	}
}

func (b *SachetBot) handleScanCommand(evt *events.Message) {
	chat := evt.Info.Chat
	chatKey := chat.String()

	b.scanMu.Lock()
	if _, exists := b.activeScans[chatKey]; exists {
		b.scanMu.Unlock()
		_ = b.sendText(chat, "Scan is already running for this group.")
		return
	}
	session := &scanSession{resultCh: make(chan scanBatchResult, 4)}
	b.activeScans[chatKey] = session
	b.scanMu.Unlock()

	if err := b.store.ResetGroup(chatKey); err != nil {
		b.log.Warnf("failed to reset group before scan: %v", err)
	}
	if err := b.store.SetScanEnabled(chatKey, true); err != nil {
		b.log.Warnf("failed to enable scan: %v", err)
	}

	groupName := b.refreshGroupMetadata(chat)
	ack := fmt.Sprintf("Starting full scan for %s.\nI will backfill history in chunks and keep this group in constant scan mode.", groupName)
	_ = b.sendText(chat, ack)

	go b.runOnDemandScan(chat, evt.Info)
}

func (b *SachetBot) runOnDemandScan(chat types.JID, seed types.MessageInfo) {
	defer func() {
		b.scanMu.Lock()
		delete(b.activeScans, chat.String())
		b.scanMu.Unlock()
	}()

	cursor := seed
	totalHistory := 0
	chunks := 0
	endReached := false

	for i := 0; i < b.cfg.HistoryMaxRequests; i++ {
		req := b.client.BuildHistorySyncRequest(&cursor, b.cfg.HistoryBatchSize)
		if _, err := b.client.SendPeerMessage(context.Background(), req); err != nil {
			b.log.Warnf("history sync request failed for %s: %v", chat, err)
			break
		}

		res, ok := b.waitForScanResult(chat.String(), 25*time.Second)
		if !ok {
			b.log.Warnf("timeout waiting for history sync result for %s", chat)
			break
		}
		chunks++
		totalHistory += res.Processed

		if res.Processed == 0 || res.Oldest == nil {
			endReached = true
			break
		}
		cursor = *res.Oldest
		if res.EndOfChat {
			endReached = true
			break
		}
	}

	b.markDirty()
	b.maybeFlush(true)
	b.sendScanSummary(chat, totalHistory, chunks, endReached)
}

func (b *SachetBot) waitForScanResult(chatKey string, timeout time.Duration) (scanBatchResult, bool) {
	b.scanMu.Lock()
	session := b.activeScans[chatKey]
	b.scanMu.Unlock()

	if session == nil {
		return scanBatchResult{}, false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case res := <-session.resultCh:
		return res, true
	case <-timer.C:
		return scanBatchResult{}, false
	}
}

func (b *SachetBot) notifyScanResult(chatKey string, result scanBatchResult) {
	b.scanMu.Lock()
	session := b.activeScans[chatKey]
	b.scanMu.Unlock()
	if session == nil {
		return
	}

	select {
	case session.resultCh <- result:
	default:
	}
}

func (b *SachetBot) sendScanSummary(chat types.JID, historyCount, chunks int, endReached bool) {
	group, _ := b.store.SnapshotGroup(chat.String())
	if group == nil {
		_ = b.sendText(chat, "Scan completed, but no stats were produced.")
		return
	}

	topLine := "No message activity found yet."
	leaders := b.store.Leaderboard(chat.String(), 1)
	if len(leaders) > 0 {
		name := renderMemberName(leaders[0].Name, leaders[0].JID)
		topLine = fmt.Sprintf("Top messenger: %s (%d messages)", name, leaders[0].Count)
	}

	endLine := "Reached currently available history."
	if !endReached {
		endLine = "Stopped due to scan request limit; run ?scan again for deeper history."
	}

	text := fmt.Sprintf(
		"Scan completed.\nGroup: %s\nHistory chunks: %d\nHistory messages indexed: %d\nTotal messages indexed: %d\nMessages/day keys: %d\n%s\nRules: only-admins-message=%t, only-admins-edit=%t, join-approval=%t\n%s",
		group.Name,
		chunks,
		historyCount,
		group.TotalMessages,
		len(group.MessagesPerDay),
		topLine,
		group.Rules.OnlyAdminsCanMessage,
		group.Rules.OnlyAdminsCanEditInfo,
		group.Rules.JoinApprovalRequired,
		endLine,
	)
	_ = b.sendText(chat, text)
}

func (b *SachetBot) handleListCommand(evt *events.Message) {
	info, err := b.client.GetGroupInfo(context.Background(), evt.Info.Chat)
	if err != nil {
		_ = b.sendText(evt.Info.Chat, fmt.Sprintf("Failed to fetch group members: %v", err))
		return
	}

	b.updateGroupMetadataFromInfo(evt.Info.Chat, info)

	lines := []string{fmt.Sprintf("Members in %s (%d):", info.Name, len(info.Participants))}
	maxRows := 120
	for i, participant := range info.Participants {
		if i >= maxRows {
			lines = append(lines, fmt.Sprintf("... and %d more", len(info.Participants)-maxRows))
			break
		}
		role := ""
		if participant.IsSuperAdmin {
			role = " [superadmin]"
		} else if participant.IsAdmin {
			role = " [admin]"
		}
		lines = append(lines, fmt.Sprintf("%d. %s%s", i+1, renderMemberName(participant.DisplayName, participant.JID.String()), role))
	}
	_ = b.sendText(evt.Info.Chat, strings.Join(lines, "\n"))
}

func (b *SachetBot) handleStatsCommand(evt *events.Message, arg string) {
	group, ok := b.store.SnapshotGroup(evt.Info.Chat.String())
	if !ok {
		_ = b.sendText(evt.Info.Chat, "No stats yet for this group. Use ?scan first.")
		return
	}

	memberJID := b.store.ResolveMember(evt.Info.Chat.String(), arg, evt.Info.Sender.String())
	member, ok := group.Members[memberJID]
	if !ok {
		_ = b.sendText(evt.Info.Chat, "No stats found for that member yet.")
		return
	}

	avgPerDay := float64(member.TotalMessages)
	daysActive := 1.0
	if !member.FirstSeen.IsZero() && !member.LastSeen.IsZero() && member.LastSeen.After(member.FirstSeen) {
		daysActive = member.LastSeen.Sub(member.FirstSeen).Hours()/24 + 1
		avgPerDay = float64(member.TotalMessages) / daysActive
	}

	lines := []string{
		fmt.Sprintf("Stats for %s", renderMemberName(member.Name, member.JID)),
		fmt.Sprintf("Total messages: %d", member.TotalMessages),
		fmt.Sprintf("Active days: %.0f", daysActive),
		fmt.Sprintf("Avg/day: %.2f", avgPerDay),
		fmt.Sprintf("First seen: %s", member.FirstSeen.Format(time.RFC3339)),
		fmt.Sprintf("Last seen: %s", member.LastSeen.Format(time.RFC3339)),
	}

	topDays := topDays(member.MessagesPerDay, 5)
	if len(topDays) > 0 {
		lines = append(lines, "Top days:")
		for _, day := range topDays {
			lines = append(lines, fmt.Sprintf("- %s: %d", day.Day, day.Count))
		}
	}

	_ = b.sendText(evt.Info.Chat, strings.Join(lines, "\n"))
}

func (b *SachetBot) handleLeaderboardCommand(evt *events.Message) {
	group, ok := b.store.SnapshotGroup(evt.Info.Chat.String())
	if !ok {
		_ = b.sendText(evt.Info.Chat, "No stats yet for this group. Use ?scan first.")
		return
	}

	top := b.store.Leaderboard(evt.Info.Chat.String(), 15)
	if len(top) == 0 {
		_ = b.sendText(evt.Info.Chat, "No leaderboard yet.")
		return
	}

	lines := []string{
		fmt.Sprintf("Leaderboard: %s", group.Name),
		fmt.Sprintf("Total indexed messages: %d", group.TotalMessages),
	}
	var mentionedJIDs []string
	for i, row := range top {
		phoneJID := b.resolveToPhoneJID(row.JID)
		parsed, err := types.ParseJID(phoneJID)
		if err == nil && parsed.Server == types.DefaultUserServer {
			lines = append(lines, fmt.Sprintf("%d. @%s - %d", i+1, parsed.User, row.Count))
			mentionedJIDs = append(mentionedJIDs, phoneJID)
		} else {
			lines = append(lines, fmt.Sprintf("%d. %s - %d", i+1, renderMemberName(row.Name, row.JID), row.Count))
		}
	}

	if len(mentionedJIDs) > 0 {
		_ = b.sendTextWithMentions(evt.Info.Chat, strings.Join(lines, "\n"), mentionedJIDs)
	} else {
		_ = b.sendText(evt.Info.Chat, strings.Join(lines, "\n"))
	}
}

func (b *SachetBot) refreshGroupMetadata(chat types.JID) string {
	info, err := b.client.GetGroupInfo(context.Background(), chat)
	if err != nil {
		b.log.Warnf("failed to refresh group metadata: %v", err)
		return chat.String()
	}
	b.updateGroupMetadataFromInfo(chat, info)
	if err := b.store.Save(); err != nil {
		b.log.Warnf("failed to save metadata refresh: %v", err)
	}
	if info.Name == "" {
		return chat.String()
	}
	return info.Name
}

func (b *SachetBot) updateGroupMetadataFromInfo(chat types.JID, info *types.GroupInfo) {
	if info == nil {
		return
	}

	adminCount := 0
	superAdminCount := 0
	for _, p := range info.Participants {
		if p.IsAdmin {
			adminCount++
		}
		if p.IsSuperAdmin {
			superAdminCount++
		}
	}
	rules := GroupRules{
		OnlyAdminsCanMessage:  info.IsAnnounce,
		OnlyAdminsCanEditInfo: info.IsLocked,
		JoinApprovalRequired:  info.IsJoinApprovalRequired,
		DisappearingTimerSec:  info.DisappearingTimer,
	}
	if err := b.store.UpdateGroupMetadata(chat.String(), info.Name, rules, adminCount, superAdminCount, len(info.Participants)); err != nil {
		b.log.Warnf("failed to update group metadata: %v", err)
	}
}

func (b *SachetBot) sendText(chat types.JID, text string) error {
	_, err := b.client.SendMessage(context.Background(), chat, &waE2E.Message{
		Conversation: proto.String(text),
	})
	if err != nil {
		b.log.Warnf("failed to send message to %s: %v", chat, err)
	}
	return err
}

func (b *SachetBot) sendTextWithMentions(chat types.JID, text string, mentionedJIDs []string) error {
	ctx := context.Background()
	msg := &waE2E.Message{
		ExtendedTextMessage: &waE2E.ExtendedTextMessage{
			Text: proto.String(text),
			ContextInfo: &waE2E.ContextInfo{
				MentionedJID: mentionedJIDs,
			},
		},
	}
	_, err := b.client.SendMessage(ctx, chat, msg)
	if err != nil {
		b.log.Warnf("failed to send message with mentions to %s: %v", chat, err)
	}
	return err
}

func (b *SachetBot) resolveToPhoneJID(jid string) string {
	parsed, err := types.ParseJID(jid)
	if err != nil {
		return jid
	}
	if parsed.Server == types.DefaultUserServer {
		return jid
	}
	if parsed.Server == types.HiddenUserServer && b.client.Store.LIDs != nil {
		pn, err := b.client.Store.LIDs.GetPNForLID(context.Background(), parsed)
		if err == nil && pn.Server == types.DefaultUserServer {
			return pn.String()
		}
	}
	return jid
}

func (b *SachetBot) markDirty() {
	b.saveMu.Lock()
	b.dirty = true
	b.saveMu.Unlock()
}

func (b *SachetBot) maybeFlush(force bool) {
	b.saveMu.Lock()
	if !b.dirty {
		b.saveMu.Unlock()
		return
	}
	if !force && time.Since(b.lastSaved) < 3*time.Second {
		b.saveMu.Unlock()
		return
	}
	b.dirty = false
	b.lastSaved = time.Now()
	b.saveMu.Unlock()

	if err := b.store.Save(); err != nil {
		b.log.Warnf("failed to flush stats store: %v", err)
		b.markDirty()
	}
}

func isCountableMessage(msg *waE2E.Message) bool {
	if msg == nil {
		return false
	}
	if msg.GetProtocolMessage() != nil {
		return false
	}
	return true
}

func extractMessageText(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	if txt := strings.TrimSpace(msg.GetConversation()); txt != "" {
		return txt
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return strings.TrimSpace(ext.GetText())
	}
	if img := msg.GetImageMessage(); img != nil {
		return strings.TrimSpace(img.GetCaption())
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		return strings.TrimSpace(vid.GetCaption())
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		return strings.TrimSpace(doc.GetCaption())
	}
	return ""
}

func renderMemberName(name, jid string) string {
	name = strings.TrimSpace(name)
	cleanJID := cleanJIDForDisplay(jid)
	if name == "" {
		return cleanJID
	}
	if name == cleanJID {
		return name
	}
	return fmt.Sprintf("%s (%s)", name, cleanJID)
}

func cleanJIDForDisplay(jid string) string {
	// Strip @s.whatsapp.net or @lid suffix
	base := jid
	if at := strings.Index(jid, "@"); at >= 0 {
		base = jid[:at]
	}
	// Strip device ID suffix (e.g. "52854672892136:4" -> "52854672892136")
	if colon := strings.Index(base, ":"); colon >= 0 {
		base = base[:colon]
	}
	return base
}

type dayStat struct {
	Day   string
	Count int64
}

func topDays(in map[string]int64, limit int) []dayStat {
	rows := make([]dayStat, 0, len(in))
	for day, count := range in {
		rows = append(rows, dayStat{Day: day, Count: count})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count == rows[j].Count {
			return rows[i].Day > rows[j].Day
		}
		return rows[i].Count > rows[j].Count
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}
