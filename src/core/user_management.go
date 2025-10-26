package mindb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Privilege represents a database privilege
type Privilege string

const (
	PrivilegeSelect Privilege = "SELECT"
	PrivilegeInsert Privilege = "INSERT"
	PrivilegeUpdate Privilege = "UPDATE"
	PrivilegeDelete Privilege = "DELETE"
	PrivilegeCreate Privilege = "CREATE"
	PrivilegeDrop   Privilege = "DROP"
	PrivilegeAll    Privilege = "ALL"
)

// User represents a database user
type User struct {
	Username       string
	PasswordHash   string
	Host           string // e.g., '%', 'localhost', '192.168.1.%'
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Locked         bool      // Account locked status
	FailedAttempts int       // Failed login attempts
	LastFailedAt   time.Time // Last failed login time
	LockedAt       time.Time // When account was locked
}

// Grant represents a privilege grant
type Grant struct {
	Username   string
	Host       string
	Database   string // '*' for all databases
	Table      string // '*' for all tables
	Privileges []Privilege
	GrantedAt  time.Time
}

// Role represents a named set of privileges
type Role struct {
	Name        string
	Description string
	Grants      []Grant
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// UserRole represents a role assigned to a user
type UserRole struct {
	Username  string
	Host      string
	RoleName  string
	GrantedAt time.Time
}

// UserManager manages users and their privileges
type UserManager struct {
	users                map[string]*User     // key: "username@host"
	grants               map[string][]Grant   // key: "username@host"
	roles                map[string]*Role     // key: role name
	userRoles            map[string][]string  // key: "username@host", value: role names
	dataDir              string               // Directory to store user data
	maxFailedAttempts    int                  // Max failed attempts before locking
	lockoutDuration      time.Duration        // Auto-unlock duration
	mu                   sync.RWMutex
}

// UserData represents serializable user data
type UserData struct {
	Users     map[string]*User     `json:"users"`
	Grants    map[string][]Grant   `json:"grants"`
	Roles     map[string]*Role     `json:"roles"`
	UserRoles map[string][]string  `json:"user_roles"`
}

// NewUserManager creates a new user manager
func NewUserManager() *UserManager {
	um := &UserManager{
		users:             make(map[string]*User),
		grants:            make(map[string][]Grant),
		roles:             make(map[string]*Role),
		userRoles:         make(map[string][]string),
		maxFailedAttempts: 5,                // Lock after 5 failed attempts
		lockoutDuration:   15 * time.Minute, // Auto-unlock after 15 minutes
	}
	
	// Create default root user
	um.createDefaultRootUser()
	
	// Create default roles
	um.createDefaultRoles()
	
	return um
}

// createDefaultRootUser creates a default root user with all privileges
func (um *UserManager) createDefaultRootUser() {
	rootUser := &User{
		Username:     "root",
		PasswordHash: hashPassword("root"), // Default password, should be changed
		Host:         "%",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	
	key := userKey(rootUser.Username, rootUser.Host)
	um.users[key] = rootUser
	
	// Grant all privileges to root
	um.grants[key] = []Grant{
		{
			Username:   "root",
			Host:       "%",
			Database:   "*",
			Table:      "*",
			Privileges: []Privilege{PrivilegeAll},
			GrantedAt:  time.Now(),
		},
	}
}

// CreateUser creates a new user
func (um *UserManager) CreateUser(username, password, host string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	if _, exists := um.users[key]; exists {
		return fmt.Errorf("user '%s'@'%s' already exists", username, host)
	}
	
	user := &User{
		Username:     username,
		PasswordHash: hashPassword(password),
		Host:         host,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	
	um.users[key] = user
	um.grants[key] = []Grant{} // Initialize empty grants
	
	return nil
}

// DropUser removes a user
func (um *UserManager) DropUser(username, host string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	if _, exists := um.users[key]; !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	delete(um.users, key)
	delete(um.grants, key)
	
	return nil
}

// Authenticate verifies user credentials and handles account locking
func (um *UserManager) Authenticate(username, password, host string) bool {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	// Try exact host match first
	key := userKey(username, host)
	user, exists := um.users[key]
	if !exists {
		// Try wildcard host match
		key = userKey(username, "%")
		user, exists = um.users[key]
		if !exists {
			return false
		}
	}
	
	// Check if account is locked
	if user.Locked {
		// Check if auto-unlock period has passed
		if time.Since(user.LockedAt) > um.lockoutDuration {
			// Auto-unlock
			user.Locked = false
			user.FailedAttempts = 0
		} else {
			// Still locked
			return false
		}
	}
	
	// Verify password
	if user.PasswordHash == hashPassword(password) {
		// Success - reset failed attempts
		user.FailedAttempts = 0
		user.UpdatedAt = time.Now()
		return true
	}
	
	// Failed authentication - increment counter
	user.FailedAttempts++
	user.LastFailedAt = time.Now()
	user.UpdatedAt = time.Now()
	
	// Lock account if too many failures
	if user.FailedAttempts >= um.maxFailedAttempts {
		user.Locked = true
		user.LockedAt = time.Now()
	}
	
	return false
}

// GrantPrivileges grants privileges to a user
func (um *UserManager) GrantPrivileges(username, host, database, table string, privileges []Privilege) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	if _, exists := um.users[key]; !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	grant := Grant{
		Username:   username,
		Host:       host,
		Database:   database,
		Table:      table,
		Privileges: privileges,
		GrantedAt:  time.Now(),
	}
	
	um.grants[key] = append(um.grants[key], grant)
	
	return nil
}

// RevokePrivileges revokes privileges from a user
func (um *UserManager) RevokePrivileges(username, host, database, table string, privileges []Privilege) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	if _, exists := um.users[key]; !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	grants := um.grants[key]
	newGrants := []Grant{}
	
	for _, grant := range grants {
		if grant.Database == database && grant.Table == table {
			// Remove specified privileges
			remainingPrivs := []Privilege{}
			for _, p := range grant.Privileges {
				shouldRemove := false
				for _, rp := range privileges {
					if p == rp || rp == PrivilegeAll {
						shouldRemove = true
						break
					}
				}
				if !shouldRemove {
					remainingPrivs = append(remainingPrivs, p)
				}
			}
			
			// Keep grant if it still has privileges
			if len(remainingPrivs) > 0 {
				grant.Privileges = remainingPrivs
				newGrants = append(newGrants, grant)
			}
		} else {
			newGrants = append(newGrants, grant)
		}
	}
	
	um.grants[key] = newGrants
	
	return nil
}

// HasPrivilege checks if a user has a specific privilege (including role-based privileges)
func (um *UserManager) HasPrivilege(username, host, database, table string, privilege Privilege) bool {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	key := userKey(username, host)
	
	// Check direct grants first
	grants, exists := um.grants[key]
	if !exists {
		// Try wildcard host
		key = userKey(username, "%")
		grants, exists = um.grants[key]
	}
	
	// Check direct grants
	if exists {
		for _, grant := range grants {
			// Check if grant applies to this database/table
			if !matchesPattern(grant.Database, database) {
				continue
			}
			if !matchesPattern(grant.Table, table) {
				continue
			}
			
			// Check if user has the privilege
			for _, p := range grant.Privileges {
				if p == PrivilegeAll || p == privilege {
					return true
				}
			}
		}
	}
	
	// Check role-based privileges
	userRoles := um.userRoles[key]
	for _, roleName := range userRoles {
		role, roleExists := um.roles[roleName]
		if !roleExists {
			continue
		}
		
		for _, grant := range role.Grants {
			// Check if grant applies to this database/table
			if !matchesPattern(grant.Database, database) {
				continue
			}
			if !matchesPattern(grant.Table, table) {
				continue
			}
			
			// Check if role has the privilege
			for _, p := range grant.Privileges {
				if p == PrivilegeAll || p == privilege {
					return true
				}
			}
		}
	}
	
	return false
}

// ListUsers returns all users
func (um *UserManager) ListUsers() []*User {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	users := make([]*User, 0, len(um.users))
	for _, user := range um.users {
		users = append(users, user)
	}
	
	return users
}

// ListGrants returns all grants for a user
func (um *UserManager) ListGrants(username, host string) []Grant {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	key := userKey(username, host)
	return um.grants[key]
}

// userKey generates a unique key for a user
func userKey(username, host string) string {
	return fmt.Sprintf("%s@%s", username, host)
}

// hashPassword hashes a password using SHA-256
func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// matchesPattern checks if a value matches a pattern (supports '*' wildcard)
func matchesPattern(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	return pattern == value
}

// ChangePassword changes a user's password
func (um *UserManager) ChangePassword(username, host, newPassword string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	user, exists := um.users[key]
	if !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	user.PasswordHash = hashPassword(newPassword)
	user.UpdatedAt = time.Now()
	
	return nil
}

// SetDataDir sets the data directory for persistence
func (um *UserManager) SetDataDir(dataDir string) {
	um.mu.Lock()
	defer um.mu.Unlock()
	um.dataDir = dataDir
}

// Save persists users and grants to disk
func (um *UserManager) Save() error {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	if um.dataDir == "" {
		return nil // No data directory set, skip persistence
	}
	
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(um.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	
	// Prepare data for serialization
	data := UserData{
		Users:     um.users,
		Grants:    um.grants,
		Roles:     um.roles,
		UserRoles: um.userRoles,
	}
	
	// Marshal to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal user data: %w", err)
	}
	
	// Write to file
	filePath := filepath.Join(um.dataDir, "users.json")
	if err := os.WriteFile(filePath, jsonData, 0600); err != nil {
		return fmt.Errorf("failed to write user data: %w", err)
	}
	
	return nil
}

// Load loads users and grants from disk
func (um *UserManager) Load() error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	if um.dataDir == "" {
		return nil // No data directory set, skip loading
	}
	
	filePath := filepath.Join(um.dataDir, "users.json")
	
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil // File doesn't exist, use defaults
	}
	
	// Read file
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read user data: %w", err)
	}
	
	// Unmarshal JSON
	var data UserData
	if err := json.Unmarshal(jsonData, &data); err != nil {
		return fmt.Errorf("failed to unmarshal user data: %w", err)
	}
	
	// Load users and grants
	if data.Users != nil {
		um.users = data.Users
	}
	if data.Grants != nil {
		um.grants = data.Grants
	}
	if data.Roles != nil {
		um.roles = data.Roles
	}
	if data.UserRoles != nil {
		um.userRoles = data.UserRoles
	}
	
	return nil
}

// UnlockAccount manually unlocks a user account
func (um *UserManager) UnlockAccount(username, host string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	user, exists := um.users[key]
	if !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	user.Locked = false
	user.FailedAttempts = 0
	user.UpdatedAt = time.Now()
	
	return nil
}

// IsLocked checks if an account is locked
func (um *UserManager) IsLocked(username, host string) bool {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	key := userKey(username, host)
	user, exists := um.users[key]
	if !exists {
		return false
	}
	
	return user.Locked
}

// GetFailedAttempts returns the number of failed login attempts
func (um *UserManager) GetFailedAttempts(username, host string) int {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	key := userKey(username, host)
	user, exists := um.users[key]
	if !exists {
		return 0
	}
	
	return user.FailedAttempts
}

// createDefaultRoles creates default roles
func (um *UserManager) createDefaultRoles() {
	// Readonly role
	um.roles["readonly"] = &Role{
		Name:        "readonly",
		Description: "Read-only access to all databases",
		Grants: []Grant{
			{
				Database:   "*",
				Table:      "*",
				Privileges: []Privilege{PrivilegeSelect},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	
	// ReadWrite role
	um.roles["readwrite"] = &Role{
		Name:        "readwrite",
		Description: "Read and write access to all databases",
		Grants: []Grant{
			{
				Database:   "*",
				Table:      "*",
				Privileges: []Privilege{PrivilegeSelect, PrivilegeInsert, PrivilegeUpdate, PrivilegeDelete},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	
	// Admin role
	um.roles["admin"] = &Role{
		Name:        "admin",
		Description: "Full administrative access",
		Grants: []Grant{
			{
				Database:   "*",
				Table:      "*",
				Privileges: []Privilege{PrivilegeAll},
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}

// CreateRole creates a new role
func (um *UserManager) CreateRole(name, description string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	if _, exists := um.roles[name]; exists {
		return fmt.Errorf("role '%s' already exists", name)
	}
	
	um.roles[name] = &Role{
		Name:        name,
		Description: description,
		Grants:      []Grant{},
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	
	return nil
}

// DropRole removes a role
func (um *UserManager) DropRole(name string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	if _, exists := um.roles[name]; !exists {
		return fmt.Errorf("role '%s' does not exist", name)
	}
	
	// Check if role is assigned to any users
	for userKey, roles := range um.userRoles {
		for _, roleName := range roles {
			if roleName == name {
				return fmt.Errorf("role '%s' is assigned to user '%s'", name, userKey)
			}
		}
	}
	
	delete(um.roles, name)
	return nil
}

// GrantRolePrivileges grants privileges to a role
func (um *UserManager) GrantRolePrivileges(roleName, database, table string, privileges []Privilege) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	role, exists := um.roles[roleName]
	if !exists {
		return fmt.Errorf("role '%s' does not exist", roleName)
	}
	
	grant := Grant{
		Database:   database,
		Table:      table,
		Privileges: privileges,
		GrantedAt:  time.Now(),
	}
	
	role.Grants = append(role.Grants, grant)
	role.UpdatedAt = time.Now()
	
	return nil
}

// GrantRoleToUser assigns a role to a user
func (um *UserManager) GrantRoleToUser(username, host, roleName string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	// Check if user exists
	key := userKey(username, host)
	if _, exists := um.users[key]; !exists {
		return fmt.Errorf("user '%s'@'%s' does not exist", username, host)
	}
	
	// Check if role exists
	if _, exists := um.roles[roleName]; !exists {
		return fmt.Errorf("role '%s' does not exist", roleName)
	}
	
	// Check if user already has this role
	for _, r := range um.userRoles[key] {
		if r == roleName {
			return fmt.Errorf("user '%s'@'%s' already has role '%s'", username, host, roleName)
		}
	}
	
	um.userRoles[key] = append(um.userRoles[key], roleName)
	
	return nil
}

// RevokeRoleFromUser removes a role from a user
func (um *UserManager) RevokeRoleFromUser(username, host, roleName string) error {
	um.mu.Lock()
	defer um.mu.Unlock()
	
	key := userKey(username, host)
	roles, exists := um.userRoles[key]
	if !exists {
		return fmt.Errorf("user '%s'@'%s' has no roles", username, host)
	}
	
	// Find and remove the role
	newRoles := []string{}
	found := false
	for _, r := range roles {
		if r != roleName {
			newRoles = append(newRoles, r)
		} else {
			found = true
		}
	}
	
	if !found {
		return fmt.Errorf("user '%s'@'%s' does not have role '%s'", username, host, roleName)
	}
	
	um.userRoles[key] = newRoles
	
	return nil
}

// ListRoles returns all roles
func (um *UserManager) ListRoles() []*Role {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	roles := make([]*Role, 0, len(um.roles))
	for _, role := range um.roles {
		roles = append(roles, role)
	}
	
	return roles
}

// GetUserRoles returns all roles assigned to a user
func (um *UserManager) GetUserRoles(username, host string) []string {
	um.mu.RLock()
	defer um.mu.RUnlock()
	
	key := userKey(username, host)
	return um.userRoles[key]
}
