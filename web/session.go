package web

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/AviK0928/RedisGo/internal/engine"
)

// sessionTTL is how long an abandoned session's keys survive. A visitor who
// closes the tab is gone, but their keys are not, so something has to reclaim
// them or the demo fills up with the leavings of everyone who ever visited.
const sessionTTL = 15 * time.Minute

// sessionManager owns the live playground sessions.
type sessionManager struct {
	engine *engine.Engine
	limits engine.SessionLimits

	mu       sync.Mutex
	sessions map[string]*trackedSession
}

type trackedSession struct {
	session  *engine.Session
	lastSeen time.Time
}

func newSessionManager(e *engine.Engine, limits engine.SessionLimits) *sessionManager {
	return &sessionManager{
		engine:   e,
		limits:   limits,
		sessions: make(map[string]*trackedSession),
	}
}

// create makes a session with an unpredictable id. Guessable ids would let one
// visitor reconnect as another and read their keys.
func (m *sessionManager) create() (*engine.Session, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(raw)

	session := m.engine.NewSession(id, m.limits)

	m.mu.Lock()
	m.sessions[id] = &trackedSession{session: session, lastSeen: time.Now()}
	m.mu.Unlock()

	return session, nil
}

func (m *sessionManager) touch(id string) {
	m.mu.Lock()
	if tracked, found := m.sessions[id]; found {
		tracked.lastSeen = time.Now()
	}
	m.mu.Unlock()
}

func (m *sessionManager) remove(id string) {
	m.mu.Lock()
	tracked, found := m.sessions[id]
	delete(m.sessions, id)
	m.mu.Unlock()

	if found {
		tracked.session.Close()
	}
}

func (m *sessionManager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// reap deletes sessions nobody has touched recently.
func (m *sessionManager) reap() {
	cutoff := time.Now().Add(-sessionTTL)

	m.mu.Lock()
	var stale []*trackedSession
	for id, tracked := range m.sessions {
		if tracked.lastSeen.Before(cutoff) {
			stale = append(stale, tracked)
			delete(m.sessions, id)
		}
	}
	m.mu.Unlock()

	// Close outside the lock: Close walks the keyspace, and holding the
	// manager lock through it would block every new connection meanwhile.
	for _, tracked := range stale {
		tracked.session.Close()
	}
}

// StartReaper runs the cleanup loop until the channel closes.
func (m *sessionManager) StartReaper(done <-chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			m.reap()
		}
	}
}
