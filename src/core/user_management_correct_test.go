package mindb

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ============================================================================
// USER MANAGEMENT TESTS (Correct API)
// ============================================================================

func TestUserManager_Creation(t *testing.T) {
	um := NewUserManager()
	if um == nil {
		t.Fatal("NewUserManager returned nil")
	}
	
	if um.users == nil {
		t.Error("Users map should be initialized")
	}
	
	// Root user should exist with wildcard host
	rootKey := "root@%"
	if _, exists := um.users[rootKey]; !exists {
		t.Error("Default root user should be created")
	}
}

func TestUserManager_CreateUser(t *testing.T) {
	um := NewUserManager()
	
	err := um.CreateUser("testuser", "password123", "localhost")
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}
	
	// Verify user exists
	userKey := "testuser@localhost"
	user, exists := um.users[userKey]
	if !exists {
		t.Fatal("User should exist after creation")
	}
	
	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", user.Username)
	}
	
	if user.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", user.Host)
	}
	
	if user.PasswordHash == "" {
		t.Error("Password hash should not be empty")
	}
}

func TestUserManager_CreateDuplicateUser(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password123", "localhost")
	
	// Try to create duplicate
	err := um.CreateUser("testuser", "password456", "localhost")
	if err == nil {
		t.Error("Should fail when creating duplicate user")
	}
}

func TestUserManager_DropUser(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password123", "localhost")
	
	err := um.DropUser("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to drop user: %v", err)
	}
	
	// Verify user doesn't exist
	userKey := "testuser@localhost"
	if _, exists := um.users[userKey]; exists {
		t.Error("User should not exist after drop")
	}
}

func TestUserManager_DropNonExistentUser(t *testing.T) {
	um := NewUserManager()
	
	err := um.DropUser("nonexistent", "localhost")
	if err == nil {
		t.Error("Should fail when dropping non-existent user")
	}
}

func TestUserManager_Authenticate(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password123", "localhost")
	
	// Correct password
	authenticated := um.Authenticate("testuser", "password123", "localhost")
	if !authenticated {
		t.Error("Authentication should succeed with correct password")
	}
	
	// Wrong password
	authenticated = um.Authenticate("testuser", "wrongpassword", "localhost")
	if authenticated {
		t.Error("Authentication should fail with wrong password")
	}
	
	// Non-existent user
	authenticated = um.Authenticate("nonexistent", "password", "localhost")
	if authenticated {
		t.Error("Authentication should fail for non-existent user")
	}
}

func TestUserManager_ChangePassword(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "oldpassword", "localhost")
	
	err := um.ChangePassword("testuser", "localhost", "newpassword")
	if err != nil {
		t.Fatalf("Failed to change password: %v", err)
	}
	
	// Old password should not work
	if um.Authenticate("testuser", "oldpassword", "localhost") {
		t.Error("Old password should not work")
	}
	
	// New password should work
	if !um.Authenticate("testuser", "newpassword", "localhost") {
		t.Error("New password should work")
	}
}

func TestUserManager_GrantPrivileges(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	privileges := []Privilege{PrivilegeSelect, PrivilegeInsert}
	err := um.GrantPrivileges("testuser", "localhost", "testdb", "users", privileges)
	if err != nil {
		t.Fatalf("Failed to grant privileges: %v", err)
	}
	
	// Verify privileges
	hasPriv := um.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeSelect)
	if !hasPriv {
		t.Error("User should have SELECT privilege")
	}
	
	hasPriv = um.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeInsert)
	if !hasPriv {
		t.Error("User should have INSERT privilege")
	}
	
	hasPriv = um.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeDelete)
	if hasPriv {
		t.Error("User should not have DELETE privilege")
	}
}

func TestUserManager_RevokePrivileges(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Grant privileges
	privileges := []Privilege{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate}
	um.GrantPrivileges("testuser", "localhost", "testdb", "users", privileges)
	
	// Revoke some privileges
	revokePrivs := []Privilege{PrivilegeInsert, PrivilegeUpdate}
	err := um.RevokePrivileges("testuser", "localhost", "testdb", "users", revokePrivs)
	if err != nil {
		t.Fatalf("Failed to revoke privileges: %v", err)
	}
	
	// Verify
	hasPriv := um.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeSelect)
	if !hasPriv {
		t.Error("User should still have SELECT privilege")
	}
	
	hasPriv = um.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeInsert)
	if hasPriv {
		t.Error("User should not have INSERT privilege after revoke")
	}
}

func TestUserManager_GrantAllPrivileges(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	privileges := []Privilege{PrivilegeAll}
	um.GrantPrivileges("testuser", "localhost", "*", "*", privileges)
	
	// Should have all privileges
	for _, priv := range []Privilege{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate, PrivilegeDelete} {
		if !um.HasPrivilege("testuser", "localhost", "anydb", "anytable", priv) {
			t.Errorf("User should have %s privilege with ALL grant", priv)
		}
	}
}

func TestUserManager_ListGrants(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	privileges := []Privilege{PrivilegeSelect, PrivilegeInsert}
	um.GrantPrivileges("testuser", "localhost", "testdb", "users", privileges)
	
	grants := um.ListGrants("testuser", "localhost")
	if len(grants) == 0 {
		t.Error("Expected at least one grant")
	}
	
	found := false
	for _, grant := range grants {
		if grant.Database == "testdb" && grant.Table == "users" {
			found = true
			if len(grant.Privileges) != 2 {
				t.Errorf("Expected 2 privileges, got %d", len(grant.Privileges))
			}
		}
	}
	
	if !found {
		t.Error("Expected grant for testdb.users not found")
	}
}

func TestUserManager_CreateRole(t *testing.T) {
	um := NewUserManager()
	
	err := um.CreateRole("developer", "Developer role")
	if err != nil {
		t.Fatalf("Failed to create role: %v", err)
	}
	
	// Verify role exists
	role, exists := um.roles["developer"]
	if !exists {
		t.Fatal("Role should exist after creation")
	}
	
	if role.Name != "developer" {
		t.Errorf("Expected role name 'developer', got '%s'", role.Name)
	}
}

func TestUserManager_DropRole(t *testing.T) {
	um := NewUserManager()
	
	um.CreateRole("developer", "Developer role")
	
	err := um.DropRole("developer")
	if err != nil {
		t.Fatalf("Failed to drop role: %v", err)
	}
	
	// Verify role doesn't exist
	if _, exists := um.roles["developer"]; exists {
		t.Error("Role should not exist after drop")
	}
}

func TestUserManager_GrantRoleToUser(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	um.CreateRole("developer", "Developer role")
	
	err := um.GrantRoleToUser("testuser", "localhost", "developer")
	if err != nil {
		t.Fatalf("Failed to grant role: %v", err)
	}
	
	// Verify user has role
	roles := um.GetUserRoles("testuser", "localhost")
	if len(roles) == 0 {
		t.Error("User should have at least one role")
	}
	
	found := false
	for _, role := range roles {
		if role == "developer" {
			found = true
		}
	}
	
	if !found {
		t.Error("User should have developer role")
	}
}

func TestUserManager_RevokeRoleFromUser(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	um.CreateRole("developer", "Developer role")
	um.GrantRoleToUser("testuser", "localhost", "developer")
	
	err := um.RevokeRoleFromUser("testuser", "localhost", "developer")
	if err != nil {
		t.Fatalf("Failed to revoke role: %v", err)
	}
	
	// Verify user doesn't have role
	roles := um.GetUserRoles("testuser", "localhost")
	for _, role := range roles {
		if role == "developer" {
			t.Error("User should not have developer role after revoke")
		}
	}
}

func TestUserManager_ListUsers(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("user1", "password", "localhost")
	um.CreateUser("user2", "password", "localhost")
	um.CreateUser("user3", "password", "%")
	
	users := um.ListUsers()
	
	// Should have at least 4 users (root + 3 created)
	if len(users) < 4 {
		t.Errorf("Expected at least 4 users, got %d", len(users))
	}
}

func TestUserManager_ListRoles(t *testing.T) {
	um := NewUserManager()
	
	um.CreateRole("role1", "Role 1")
	um.CreateRole("role2", "Role 2")
	
	roles := um.ListRoles()
	
	// Should have at least 2 roles (may have default roles)
	if len(roles) < 2 {
		t.Errorf("Expected at least 2 roles, got %d", len(roles))
	}
}

func TestUserManager_AccountLocking(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Simulate failed login attempts
	for i := 0; i < 5; i++ {
		um.Authenticate("testuser", "wrongpassword", "localhost")
	}
	
	// Account should be locked
	userKey := "testuser@localhost"
	user := um.users[userKey]
	if !user.Locked {
		t.Error("Account should be locked after 5 failed attempts")
	}
	
	// Even correct password should fail
	authenticated := um.Authenticate("testuser", "password", "localhost")
	if authenticated {
		t.Error("Authentication should fail for locked account")
	}
}

func TestUserManager_UnlockAccount(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Lock account
	for i := 0; i < 5; i++ {
		um.Authenticate("testuser", "wrongpassword", "localhost")
	}
	
	// Unlock
	err := um.UnlockAccount("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to unlock account: %v", err)
	}
	
	// Should be able to authenticate now
	authenticated := um.Authenticate("testuser", "password", "localhost")
	if !authenticated {
		t.Error("Authentication should succeed after unlock")
	}
}

func TestUserManager_IsLocked(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Initially not locked
	if um.IsLocked("testuser", "localhost") {
		t.Error("Account should not be locked initially")
	}
	
	// Lock account
	for i := 0; i < 5; i++ {
		um.Authenticate("testuser", "wrongpassword", "localhost")
	}
	
	// Should be locked
	if !um.IsLocked("testuser", "localhost") {
		t.Error("Account should be locked after failed attempts")
	}
}

func TestUserManager_GetFailedAttempts(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Initially 0
	attempts := um.GetFailedAttempts("testuser", "localhost")
	if attempts != 0 {
		t.Errorf("Expected 0 failed attempts, got %d", attempts)
	}
	
	// Fail once
	um.Authenticate("testuser", "wrongpassword", "localhost")
	
	attempts = um.GetFailedAttempts("testuser", "localhost")
	if attempts != 1 {
		t.Errorf("Expected 1 failed attempt, got %d", attempts)
	}
}

func TestUserManager_SaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	
	um := NewUserManager()
	um.SetDataDir(tmpDir)
	
	// Create some users and grants
	um.CreateUser("testuser", "password", "localhost")
	um.GrantPrivileges("testuser", "localhost", "testdb", "users", []Privilege{PrivilegeSelect})
	um.CreateRole("developer", "Developer role")
	um.GrantRoleToUser("testuser", "localhost", "developer")
	
	// Save
	err := um.Save()
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}
	
	// Verify file exists
	userFile := filepath.Join(tmpDir, "users.json")
	if _, err := os.Stat(userFile); os.IsNotExist(err) {
		t.Error("User file should exist")
	}
	
	// Create new manager and load
	um2 := NewUserManager()
	um2.SetDataDir(tmpDir)
	err = um2.Load()
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}
	
	// Verify data was loaded
	userKey := "testuser@localhost"
	if _, exists := um2.users[userKey]; !exists {
		t.Error("User should exist after load")
	}
	
	// Verify grants
	hasPriv := um2.HasPrivilege("testuser", "localhost", "testdb", "users", PrivilegeSelect)
	if !hasPriv {
		t.Error("Privileges not loaded correctly")
	}
	
	// Verify roles
	roles := um2.GetUserRoles("testuser", "localhost")
	if len(roles) == 0 {
		t.Error("Roles not loaded correctly")
	}
}

func TestUserManager_WildcardHost(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "%")
	
	// Should authenticate from any host
	if !um.Authenticate("testuser", "password", "192.168.1.1") {
		t.Error("Should authenticate with wildcard host")
	}
	
	if !um.Authenticate("testuser", "password", "10.0.0.1") {
		t.Error("Should authenticate with wildcard host")
	}
}

func TestUserManager_PrivilegeInheritance(t *testing.T) {
	um := NewUserManager()
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Grant on all databases
	um.GrantPrivileges("testuser", "localhost", "*", "*", []Privilege{PrivilegeSelect})
	
	// Should have privilege on any database/table
	hasPriv := um.HasPrivilege("testuser", "localhost", "anydb", "anytable", PrivilegeSelect)
	if !hasPriv {
		t.Error("Should have privilege on any database with * grant")
	}
}

func TestUserManager_RootUser(t *testing.T) {
	um := NewUserManager()
	
	// Root user should exist with wildcard host
	rootKey := "root@%"
	root, exists := um.users[rootKey]
	if !exists {
		t.Fatal("Root user should exist")
	}
	
	if root.Username != "root" {
		t.Error("Root username should be 'root'")
	}
	
	// Root should have all privileges (wildcard host matches any host)
	hasPriv := um.HasPrivilege("root", "%", "anydb", "anytable", PrivilegeAll)
	if !hasPriv {
		t.Error("Root should have ALL privileges")
	}
}

func TestUserManager_ConcurrentAccess(t *testing.T) {
	um := NewUserManager()
	
	done := make(chan bool)
	
	// Concurrent user creation
	for i := 0; i < 10; i++ {
		go func(id int) {
			username := "user" + string(rune('0'+id))
			um.CreateUser(username, "password", "localhost")
			done <- true
		}(i)
	}
	
	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify all users created
	users := um.ListUsers()
	if len(users) < 11 { // 10 + root
		t.Errorf("Expected at least 11 users, got %d", len(users))
	}
}

func TestUserManager_AutoUnlock(t *testing.T) {
	um := NewUserManager()
	um.lockoutDuration = 100 * time.Millisecond // Short duration for testing
	
	um.CreateUser("testuser", "password", "localhost")
	
	// Lock account
	for i := 0; i < 5; i++ {
		um.Authenticate("testuser", "wrongpassword", "localhost")
	}
	
	// Wait for auto-unlock
	time.Sleep(150 * time.Millisecond)
	
	// Should be able to authenticate now
	authenticated := um.Authenticate("testuser", "password", "localhost")
	if !authenticated {
		t.Error("Account should auto-unlock after duration")
	}
}

func TestPrivilegeConstants(t *testing.T) {
	privileges := []Privilege{
		PrivilegeSelect,
		PrivilegeInsert,
		PrivilegeUpdate,
		PrivilegeDelete,
		PrivilegeCreate,
		PrivilegeDrop,
		PrivilegeAll,
	}
	
	for _, priv := range privileges {
		if string(priv) == "" {
			t.Error("Privilege constant should not be empty")
		}
	}
}
