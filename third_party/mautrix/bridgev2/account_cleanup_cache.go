package bridgev2

import (
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// AccountCleanupRunLocked 在桥接缓存锁内执行账号事务，避免同一登录被重新缓存。
// 不能先拿 deleteLock 再拿 cacheLock：NewLogin 的既有路径按相反顺序加锁，会形成死锁。
func (br *Bridge) AccountCleanupRunLocked(userMXID id.UserID, login *UserLogin, cleanup func() ([]networkid.PortalKey, bool, error)) error {
	br.cacheLock.Lock()
	keys, clearUser, err := cleanup()
	if err != nil {
		br.cacheLock.Unlock()
		return err
	}
	for _, key := range keys {
		if portal := br.portalsByKey[key]; portal != nil {
			portal.unlockedDeleteCache()
		}
	}
	user := br.usersByMXID[userMXID]
	if login != nil {
		if br.userLoginsByID[login.ID] == login {
			delete(br.userLoginsByID, login.ID)
		}
		if login.User != nil && login.User.logins[login.ID] == login {
			delete(login.User.logins, login.ID)
		}
	}
	if clearUser && user != nil && len(user.logins) == 0 && br.usersByMXID[user.MXID] == user {
		delete(br.usersByMXID, user.MXID)
		user.doublePuppetLock.Lock()
		user.AccessToken = ""
		user.doublePuppetIntent = nil
		user.doublePuppetInitialized = false
		user.doublePuppetLock.Unlock()
	}
	br.cacheLock.Unlock()
	if login != nil && login.BridgeState != nil {
		login.BridgeState.Destroy()
		login.BridgeState = nil
	}
	return nil
}


