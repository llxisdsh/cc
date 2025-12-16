package benchmark

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/llxisdsh/cc"
)

// Session represents a user session.
type Session struct {
	UserID     string
	Data       string
	LastActive int64 // Unix timestamp
	Expiry     int64 // Seconds
}

// SessionManager manages user sessions using cc.FlatMap.
type SessionManager struct {
	sessions *cc.FlatMap[string, Session]
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: cc.NewFlatMap[string, Session](cc.WithCapacity(1024), cc.WithAutoShrink()),
	}
}

// Login creates or updates a session.
func (sm *SessionManager) Login(userID, data string, expirySeconds int64) {
	now := time.Now().UnixNano()
	sm.sessions.Compute(userID, func(e *cc.Entry[string, Session]) {
		e.Update(Session{
			UserID:     userID,
			Data:       data,
			LastActive: now,
			Expiry:     expirySeconds,
		})
	})
}

// Touch updates the LastActive timestamp if the session exists.
func (sm *SessionManager) Touch(userID string) bool {
	now := time.Now().UnixNano()
	_, loaded := sm.sessions.Compute(userID, func(e *cc.Entry[string, Session]) {
		if e.Loaded() {
			s := e.Value()
			s.LastActive = now
			e.Update(s)
		}
	})
	return loaded
}

// Logout removes a session.
func (sm *SessionManager) Logout(userID string) {
	sm.sessions.Delete(userID)
}

// GetSession retrieves a session.
func (sm *SessionManager) GetSession(userID string) (Session, bool) {
	return sm.sessions.Load(userID)
}

// Cleanup removes expired sessions.
// Returns the number of removed sessions.
func (sm *SessionManager) Cleanup() int {
	now := time.Now().UnixNano()
	removed := 0
	for e := range sm.sessions.Entries() {
		s := e.Value()
		if now-s.LastActive > s.Expiry*1e9 {
			e.Delete()
			removed++
		}
	}
	return removed
}

// ActiveCount returns the number of active sessions.
func (sm *SessionManager) ActiveCount() int {
	return sm.sessions.Size()
}

// TestComplexFlatMapScenario verifies the correctness of SessionManager.
func TestComplexFlatMapScenario(t *testing.T) {
	sm := NewSessionManager()

	// 1. Login
	userID := "user1"
	sm.Login(userID, "some-data", 3600)

	// 2. GetSession
	s, ok := sm.GetSession(userID)
	if !ok {
		t.Fatalf("expected session to exist")
	}
	if s.Data != "some-data" {
		t.Errorf("expected data 'some-data', got '%s'", s.Data)
	}

	// 3. Touch
	time.Sleep(10 * time.Millisecond) // Ensure time advances
	if !sm.Touch(userID) {
		t.Errorf("expected touch to succeed")
	}
	s2, _ := sm.GetSession(userID)
	if s2.LastActive <= s.LastActive {
		t.Errorf("expected LastActive to increase")
	}

	// 4. Cleanup (not expired)
	removed := sm.Cleanup()
	if removed != 0 {
		t.Errorf("expected 0 removed, got %d", removed)
	}

	// 5. Cleanup (expired)
	// Force expire by logging in with short expiry
	sm.Login("user2", "expired-data", -1) // Already expired
	removed = sm.Cleanup()
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}
	_, ok = sm.GetSession("user2")
	if ok {
		t.Errorf("expected user2 to be removed")
	}

	// 6. Logout
	sm.Logout(userID)
	_, ok = sm.GetSession(userID)
	if ok {
		t.Errorf("expected user1 to be removed")
	}
}

// BenchmarkComplexFlatMapScenario benchmarks the SessionManager under concurrent load.
func BenchmarkComplexFlatMapScenario(b *testing.B) {
	sm := NewSessionManager()
	// Pre-populate
	for i := 0; i < 10000; i++ {
		sm.Login(fmt.Sprintf("user-%d", i), "data", 3600)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		id := rand.IntN(20000) // 50% hit rate for existing users
		userID := fmt.Sprintf("user-%d", id)
		for pb.Next() {
			op := rand.IntN(100)
			if op < 10 { // 10% Login
				sm.Login(userID, "new-data", 3600)
			} else if op < 20 { // 10% Logout
				sm.Logout(userID)
			} else if op < 60 { // 40% Touch
				sm.Touch(userID)
			} else { // 40% Get
				sm.GetSession(userID)
			}
		}
	})
}

// BenchmarkCleanup benchmarks the Cleanup operation.
func BenchmarkCleanup(b *testing.B) {
	sm := NewSessionManager()
	// Populate with mix of expired and active
	for i := range 100000 {
		expiry := int64(3600)
		if i%2 == 0 {
			expiry = -1 // Expired
		}
		sm.Login(fmt.Sprintf("user-%d", i), "data", expiry)
	}

	b.ResetTimer()
	for i := range b.N {
		// We only run cleanup once effectively per iteration if we don't refill,
		// but for benchmarking the scan speed, we can just run it.
		// To make it repeatable, we might need to refill, but let's just measure
		// the scan speed on a large map.
		if sm.ActiveCount() < 50000 {
			b.StopTimer()
			for j := range 50000 {
				sm.Login(fmt.Sprintf("new-user-%d-%d", i, j), "data", -1)
			}
			b.StartTimer()
		}
		sm.Cleanup()
	}
}
