// Mindb Web Console - Application Logic

// Session management - using HTTP-only cookies (no credentials in JavaScript)
let sessionActive = false;
let sessionRefreshTimer = null;

class MindbClient {
    constructor() {
        this.serverUrl = '';
        this.database = '';
        this.connected = false;
        this.authenticated = false;
        this.queryHistory = this.loadHistory();
    }
    
    // Get authentication headers (session cookie sent automatically)
    getAuthHeaders() {
        // Session cookie is sent automatically by browser
        // Only include Basic Auth for initial login
        return {};
    }

    async connect(serverUrl, database = '') {
        // If serverUrl is empty or same origin, use relative URLs
        if (!serverUrl || serverUrl === window.location.origin) {
            this.serverUrl = '';
        } else {
            this.serverUrl = serverUrl;
        }
        this.database = database;
        
        try {
            // Check server health
            const healthUrl = this.serverUrl ? `${this.serverUrl}/health` : '/health';
            const healthResponse = await fetch(healthUrl, {
                headers: this.getAuthHeaders()
            });
            if (!healthResponse.ok) {
                throw new Error('Server not responding');
            }
            
            // If database is specified, verify it exists by trying to use it
            if (database && database.trim() !== '') {
                const testUrl = this.serverUrl ? `${this.serverUrl}/execute` : '/execute';
                const testResponse = await fetch(testUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Mindb-Database': database,
                        ...this.getAuthHeaders()
                    },
                    // Use a harmless query that will fail if database doesn't exist
                    body: JSON.stringify({ sql: 'BEGIN' })
                });
                
                if (!testResponse.ok) {
                    const errorData = await testResponse.json();
                    // Check if error is about database not existing
                    if (errorData.error && errorData.error.message) {
                        const errorMsg = errorData.error.message.toLowerCase();
                        if (errorMsg.includes('does not exist') || 
                            errorMsg.includes('not found') ||
                            errorMsg.includes('no such database')) {
                            throw new Error(`Database '${database}' does not exist`);
                        }
                        // If it's any other error, the database likely exists but there's another issue
                        // Let's allow the connection and let the user see the error when they query
                    }
                }
            }
            
            this.connected = true;
            return { success: true };
        } catch (error) {
            this.connected = false;
            return { success: false, error: error.message };
        }
    }

    transformSQL(sql) {
        // Note: Mindb doesn't currently support SHOW TABLES or DESCRIBE commands
        // These would need to be implemented in the server first
        // For now, we just pass through the SQL as-is
        return sql;
    }

    async createDatabase(dbName) {
        if (!this.connected) {
            throw new Error('Not connected to server');
        }

        const sql = `CREATE DATABASE ${dbName}`;
        
        try {
            const url = this.serverUrl ? `${this.serverUrl}/execute` : '/execute';
            const response = await fetch(url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    ...this.getAuthHeaders()
                },
                body: JSON.stringify({ sql })
            });

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.error?.message || 'Failed to create database');
            }

            return {
                success: true,
                data
            };
        } catch (error) {
            throw error;
        }
    }

    async executeQuery(sql) {
        if (!this.connected) {
            throw new Error('Not connected to server');
        }

        const headers = {
            'Content-Type': 'application/json',
            ...this.getAuthHeaders()
        };

        if (this.database) {
            headers['X-Mindb-Database'] = this.database;
        }

        const startTime = Date.now();
        
        try {
            // Transform MySQL-style commands to Mindb queries
            sql = this.transformSQL(sql);
            
            // Use /query for SELECT and DESCRIBE statements, /execute for others
            const sqlUpper = sql.trim().toUpperCase();
            const isQuery = sqlUpper.startsWith('SELECT') || sqlUpper.startsWith('DESCRIBE') || sqlUpper.startsWith('DESC ');
            const endpoint = isQuery ? '/query' : '/execute';
            const url = this.serverUrl ? `${this.serverUrl}${endpoint}` : endpoint;
            
            const response = await fetch(url, {
                method: 'POST',
                headers,
                body: JSON.stringify({ sql })
            });

            const data = await response.json();
            const latency = Date.now() - startTime;

            if (!response.ok) {
                throw new Error(data.error?.message || 'Query failed');
            }

            // Add to history
            this.addToHistory({
                sql,
                success: true,
                latency,
                timestamp: new Date().toISOString()
            });

            return {
                success: true,
                data,
                latency
            };
        } catch (error) {
            // Add to history
            this.addToHistory({
                sql,
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            });

            throw error;
        }
    }

    addToHistory(entry) {
        this.queryHistory.unshift(entry);
        if (this.queryHistory.length > 50) {
            this.queryHistory = this.queryHistory.slice(0, 50);
        }
        this.saveHistory();
    }

    loadHistory() {
        try {
            const history = localStorage.getItem('mindb_query_history');
            return history ? JSON.parse(history) : [];
        } catch {
            return [];
        }
    }

    saveHistory() {
        try {
            localStorage.setItem('mindb_query_history', JSON.stringify(this.queryHistory));
        } catch (error) {
            console.error('Failed to save history:', error);
        }
    }

    clearHistory() {
        this.queryHistory = [];
        this.saveHistory();
    }
}

// Application State
const client = new MindbClient();
let currentQuery = '';

// DOM Elements
const loginPanel = document.getElementById('loginPanel');
const connectionPanel = document.getElementById('connectionPanel');
const queryPanel = document.getElementById('queryPanel');
const resultsPanel = document.getElementById('resultsPanel');
const historyPanel = document.getElementById('historyPanel');
const sqlEditor = document.getElementById('sqlEditor');
const loginBtn = document.getElementById('loginBtn');
const logoutBtn = document.getElementById('logoutBtn');
const connectBtn = document.getElementById('connectBtn');
const disconnectBtn = document.getElementById('disconnectBtn');
const createDbBtn = document.getElementById('createDbBtn');
const confirmCreateDbBtn = document.getElementById('confirmCreateDbBtn');
const uploadProcedureBtn = document.getElementById('uploadProcedureBtn');
const executeBtn = document.getElementById('executeBtn');
const clearBtn = document.getElementById('clearBtn');
const exportBtn = document.getElementById('exportBtn');
const clearHistoryBtn = document.getElementById('clearHistoryBtn');
const connectionStatus = document.getElementById('connectionStatus');
const serverStatus = document.getElementById('serverStatus');
const currentDatabase = document.getElementById('currentDatabase');
const lastQueryTime = document.getElementById('lastQueryTime');
const lineCount = document.getElementById('lineCount');
const charCount = document.getElementById('charCount');

// Event Listeners
loginBtn.addEventListener('click', handleLogin);
logoutBtn.addEventListener('click', handleLogout);
connectBtn.addEventListener('click', handleConnect);
disconnectBtn.addEventListener('click', handleDisconnect);
createDbBtn.addEventListener('click', handleCreateDbClick);
confirmCreateDbBtn.addEventListener('click', handleConfirmCreateDb);
uploadProcedureBtn.addEventListener('click', handleUploadProcedure);
executeBtn.addEventListener('click', handleExecute);
clearBtn.addEventListener('click', handleClear);
exportBtn.addEventListener('click', handleExport);
clearHistoryBtn.addEventListener('click', handleClearHistory);
sqlEditor.addEventListener('input', updateEditorInfo);
sqlEditor.addEventListener('keydown', handleEditorKeydown);

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    updateEditorInfo();
    initializeTabs();
    checkExistingSession();
});

// Check for existing session on page load
async function checkExistingSession() {
    // Try to verify session by calling health endpoint
    // Session cookie will be sent automatically
    try {
        const response = await fetch('/health', {
            credentials: 'same-origin' // Include cookies
        });
        
        if (response.ok) {
            // Session is valid
            sessionActive = true;
            
            // Hide login panel, show connection panel
            loginPanel.style.display = 'none';
            connectionPanel.style.display = 'block';
            
            // Update header (username stored in localStorage for display only)
            const displayName = localStorage.getItem('mindb_display_name') || 'user';
            document.getElementById('userInfo').textContent = displayName;
            document.getElementById('userInfo').style.display = 'inline';
            logoutBtn.style.display = 'inline-block';
            
            // Start session refresh timer
            startSessionRefresh();
            
            // Auto-connect if there's a saved connection
            loadSavedConnection();
        }
    } catch (error) {
        console.log('No active session');
    }
}

// Tab Management
function initializeTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
}

function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
    });
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.style.display = 'none';
    });
    
    const activeTab = document.getElementById(tabName + 'Tab');
    if (activeTab) {
        activeTab.style.display = 'flex';
    }
    
    // Load procedures when switching to procedures tab
    if (tabName === 'procedures') {
        loadStoredProcedures();
    }
}

// Login Handler
async function handleLogin() {
    const username = document.getElementById('loginUsername').value.trim();
    const password = document.getElementById('loginPassword').value;
    const host = document.getElementById('loginHost').value.trim() || '%';
    
    if (!username || !password) {
        showLoginError('Please enter username and password');
        return;
    }
    
    loginBtn.disabled = true;
    loginBtn.textContent = 'Logging in...';
    
    try {
        // Authenticate with Basic Auth - server will set session cookie
        const response = await fetch('/health', {
            method: 'GET',
            headers: {
                'Authorization': 'Basic ' + btoa(username + ':' + password)
            },
            credentials: 'same-origin' // Include cookies in response
        });
        
        if (response.ok) {
            // Session cookie has been set by server
            sessionActive = true;
            
            // Store display name only (not credentials!)
            localStorage.setItem('mindb_display_name', `${username}@${host}`);
            
            // Hide login panel, show connection panel
            loginPanel.style.display = 'none';
            connectionPanel.style.display = 'block';
            
            // Update header
            document.getElementById('userInfo').textContent = `${username}@${host}`;
            document.getElementById('userInfo').style.display = 'inline';
            logoutBtn.style.display = 'inline-block';
            
            // Clear login form (security)
            document.getElementById('loginPassword').value = '';
            
            // Start session refresh timer
            startSessionRefresh();
            
            // Auto-connect if there's a saved connection
            loadSavedConnection();
        } else {
            const data = await response.json();
            const errorMsg = data.error?.message || 'Invalid credentials or account locked';
            showLoginError(errorMsg);
        }
    } catch (error) {
        showLoginError('Connection failed: ' + error.message);
    } finally {
        loginBtn.disabled = false;
        loginBtn.textContent = 'Login';
    }
}

// Logout Handler
async function handleLogout() {
    // Stop session refresh
    stopSessionRefresh();
    
    // Call logout endpoint to revoke session
    try {
        await fetch('/auth/logout', {
            method: 'POST',
            credentials: 'same-origin'
        });
    } catch (error) {
        console.error('Logout error:', error);
    }
    
    // Clear session state
    sessionActive = false;
    localStorage.removeItem('mindb_display_name');
    
    // Disconnect if connected
    if (client.connected) {
        handleDisconnect();
    }
    
    // Show login panel, hide connection panel
    loginPanel.style.display = 'block';
    connectionPanel.style.display = 'none';
    
    // Hide tabs
    document.getElementById('tabsContainer').style.display = 'none';
    document.querySelectorAll('.tab-content').forEach(content => {
        content.style.display = 'none';
    });
    
    // Update header
    document.getElementById('userInfo').style.display = 'none';
    logoutBtn.style.display = 'none';
    
    // Clear form
    document.getElementById('loginUsername').value = '';
    document.getElementById('loginPassword').value = '';
    document.getElementById('loginHost').value = '%';
    document.getElementById('loginError').style.display = 'none';
    
    showNotification('Logged out successfully', 'info');
}

// Session refresh management
function startSessionRefresh() {
    // Refresh session every 10 minutes (session timeout is 15 minutes)
    sessionRefreshTimer = setInterval(async () => {
        try {
            const response = await fetch('/auth/refresh', {
                method: 'POST',
                credentials: 'same-origin'
            });
            
            if (!response.ok) {
                // Session expired - force logout
                console.log('Session expired');
                stopSessionRefresh();
                handleLogout();
                showNotification('Session expired. Please login again.', 'warning');
            }
        } catch (error) {
            console.error('Session refresh error:', error);
        }
    }, 10 * 60 * 1000); // 10 minutes
}

function stopSessionRefresh() {
    if (sessionRefreshTimer) {
        clearInterval(sessionRefreshTimer);
        sessionRefreshTimer = null;
    }
}

// Show login error
function showLoginError(message) {
    const errorDiv = document.getElementById('loginError');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}

// Connection Handler
async function handleConnect() {
    const serverUrl = document.getElementById('serverUrl').value.trim();
    const database = document.getElementById('database').value.trim();

    // Empty server URL means use same origin (no CORS issues)
    const effectiveUrl = serverUrl || window.location.origin;

    connectBtn.disabled = true;
    connectBtn.textContent = 'Connecting...';

    try {
        const result = await client.connect(serverUrl, database);
        
        if (result.success) {
            updateConnectionStatus(true);
            connectionPanel.style.display = 'none';
            
            // Show tabs and switch to query tab
            document.getElementById('tabsContainer').style.display = 'flex';
            switchTab('query');
            
            // Save connection
            localStorage.setItem('mindb_server_url', serverUrl);
            localStorage.setItem('mindb_database', database);
            
            showNotification('Connected successfully!', 'success');
            sqlEditor.focus();
        } else {
            throw new Error(result.error);
        }
    } catch (error) {
        showNotification(`Connection failed: ${error.message}`, 'error');
        updateConnectionStatus(false);
    } finally {
        connectBtn.disabled = false;
        connectBtn.textContent = 'Connect';
    }
}

// Disconnect Handler
function handleDisconnect() {
    // Reset client state
    client.connected = false;
    client.serverUrl = '';
    client.database = '';
    
    // Update UI
    updateConnectionStatus(false);
    connectionPanel.style.display = 'block';
    
    // Hide tabs
    document.getElementById('tabsContainer').style.display = 'none';
    document.querySelectorAll('.tab-content').forEach(content => {
        content.style.display = 'none';
    });
    
    // Clear results
    document.getElementById('resultsContent').innerHTML = `
        <div class="empty-state">
            <p>Execute a query to see results here</p>
        </div>
    `;
    
    // Clear editor
    sqlEditor.value = '';
    updateEditorInfo();
    
    // Clear saved connection
    localStorage.removeItem('mindb_server_url');
    localStorage.removeItem('mindb_database');
    
    showNotification('Disconnected successfully', 'info');
}

// Create Database Click Handler
function handleCreateDbClick() {
    // Just show the modal - we'll connect when creating the database
    showCreateDbModal();
}

// Show Create Database Modal
function showCreateDbModal() {
    document.getElementById('newDbName').value = '';
    document.getElementById('createDbModal').classList.add('active');
    setTimeout(() => {
        document.getElementById('newDbName').focus();
    }, 100);
}

// Confirm Create Database Handler
async function handleConfirmCreateDb() {
    const dbName = document.getElementById('newDbName').value.trim();
    
    if (!dbName) {
        showNotification('Please enter a database name', 'error');
        return;
    }
    
    // Validate database name (alphanumeric and underscores only)
    if (!/^[a-zA-Z0-9_]+$/.test(dbName)) {
        showNotification('Database name can only contain letters, numbers, and underscores', 'error');
        return;
    }
    
    confirmCreateDbBtn.disabled = true;
    confirmCreateDbBtn.textContent = 'Creating...';
    
    try {
        // Connect to server first (without UI change)
        if (!client.connected) {
            const serverUrl = document.getElementById('serverUrl').value.trim();
            const healthUrl = serverUrl || '/health';
            const healthResponse = await fetch(healthUrl);
            if (!healthResponse.ok) {
                throw new Error('Server not responding');
            }
            client.serverUrl = serverUrl || '';
        }
        
        // Create the database
        const result = await client.createDatabase(dbName);
        
        closeModal('createDbModal');
        showNotification(`Database '${dbName}' created successfully!`, 'success');
        
        // Set the database name in the input field
        document.getElementById('database').value = dbName;
        
    } catch (error) {
        showNotification(`Failed to create database: ${error.message}`, 'error');
    } finally {
        confirmCreateDbBtn.disabled = false;
        confirmCreateDbBtn.textContent = 'Create Database';
    }
}

// Execute Query Handler
async function handleExecute() {
    const sql = sqlEditor.value.trim();
    
    if (!sql) {
        showNotification('Please enter a SQL query', 'error');
        return;
    }

    executeBtn.disabled = true;
    executeBtn.innerHTML = '<div class="spinner" style="width: 16px; height: 16px; margin: 0;"></div> Executing...';

    try {
        const result = await client.executeQuery(sql);
        displayResults(result);
        if (lastQueryTime) {
            lastQueryTime.textContent = `${result.latency}ms`;
        }
        showNotification(`Query executed successfully in ${result.latency}ms`, 'success');
    } catch (error) {
        displayError(error.message);
        showNotification(`Query failed: ${error.message}`, 'error');
    } finally {
        executeBtn.disabled = false;
        executeBtn.innerHTML = `
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <path d="M3 2L13 8L3 14V2Z" fill="currentColor"/>
            </svg>
            Execute
        `;
    }
}

// Display Results
function displayResults(result) {
    const resultsContent = document.getElementById('resultsContent');
    
    // Debug: log the response structure
    console.log('Response data:', result.data);
    
    // Check if we have columns and rows (SELECT query)
    if (result.data.columns && result.data.rows) {
        // SELECT query with results
        const html = `
            <div class="message message-success">
                Query executed successfully. ${result.data.rows.length} row(s) returned in ${result.latency}ms.
            </div>
            <div style="overflow-x: auto;">
                <table class="results-table">
                    <thead>
                        <tr>
                            ${result.data.columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
                        ${result.data.rows.map(row => `
                            <tr>
                                ${row.map(cell => `<td>${escapeHtml(formatValue(cell))}</td>`).join('')}
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;
        resultsContent.innerHTML = html;
    } else if (result.data.affected_rows !== undefined) {
        // Non-SELECT query with affected_rows
        resultsContent.innerHTML = `
            <div class="message message-success">
                ✅ ${result.data.affected_rows} row(s) affected
                <br><small>Executed in ${result.latency}ms</small>
            </div>
        `;
    } else if (result.data.result) {
        // Result string
        resultsContent.innerHTML = `
            <div class="message message-success">
                ${escapeHtml(result.data.result)}
                <br><small>Executed in ${result.latency}ms</small>
            </div>
        `;
    } else {
        // Unknown format - show raw data for debugging
        resultsContent.innerHTML = `
            <div class="message message-info">
                Query executed successfully in ${result.latency}ms.
            </div>
            <div class="message message-info" style="margin-top: 1rem;">
                <strong>Response:</strong>
                <pre style="margin-top: 0.5rem; padding: 0.5rem; background: var(--bg-primary); border-radius: 4px; overflow-x: auto;">${escapeHtml(JSON.stringify(result.data, null, 2))}</pre>
            </div>
        `;
    }
}

// Display Error
function displayError(error) {
    const resultsContent = document.getElementById('resultsContent');
    resultsContent.innerHTML = `
        <div class="message message-error">
            <strong>Error:</strong> ${escapeHtml(error)}
        </div>
    `;
}

// Clear Editor
function handleClear() {
    if (confirm('Clear the SQL editor?')) {
        sqlEditor.value = '';
        updateEditorInfo();
        sqlEditor.focus();
    }
}

// Export to CSV
function handleExport() {
    const resultsContent = document.getElementById('resultsContent');
    const table = resultsContent.querySelector('.results-table');
    
    if (!table) {
        showNotification('No results to export', 'error');
        return;
    }

    const csv = tableToCSV(table);
    downloadCSV(csv, 'mindb_results.csv');
    showNotification('Results exported to CSV', 'success');
}

// Table to CSV
function tableToCSV(table) {
    const rows = [];
    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent);
    rows.push(headers.join(','));

    table.querySelectorAll('tbody tr').forEach(tr => {
        const cells = Array.from(tr.querySelectorAll('td')).map(td => {
            let value = td.textContent;
            if (value.includes(',') || value.includes('"') || value.includes('\n')) {
                value = `"${value.replace(/"/g, '""')}"`;
            }
            return value;
        });
        rows.push(cells.join(','));
    });

    return rows.join('\n');
}

// Download CSV
function downloadCSV(csv, filename) {
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

// Update Editor Info
function updateEditorInfo() {
    const text = sqlEditor.value;
    const lines = text.split('\n').length;
    const chars = text.length;
    
    lineCount.textContent = `Line ${lines}`;
    charCount.textContent = `${chars} character${chars !== 1 ? 's' : ''}`;
}

// Editor Keyboard Shortcuts
function handleEditorKeydown(e) {
    // Ctrl/Cmd + Enter to execute
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        handleExecute();
    }
    
    // Tab to insert spaces
    if (e.key === 'Tab') {
        e.preventDefault();
        const start = sqlEditor.selectionStart;
        const end = sqlEditor.selectionEnd;
        sqlEditor.value = sqlEditor.value.substring(0, start) + '    ' + sqlEditor.value.substring(end);
        sqlEditor.selectionStart = sqlEditor.selectionEnd = start + 4;
    }
}

// Update Connection Status
function updateConnectionStatus(connected) {
    const statusDot = connectionStatus.querySelector('.status-dot');
    const statusText = connectionStatus.querySelector('.status-text');
    
    if (connected) {
        statusDot.classList.add('connected');
        statusText.textContent = 'Connected';
        disconnectBtn.style.display = 'inline-block';
    } else {
        statusDot.classList.remove('connected');
        statusText.textContent = 'Disconnected';
        disconnectBtn.style.display = 'none';
    }
}

// Show Notification
function showNotification(message, type = 'info') {
    // Log to console
    console.log(`[${type.toUpperCase()}] ${message}`);
    
    // Create toast container if it doesn't exist
    let toastContainer = document.getElementById('toastContainer');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toastContainer';
        toastContainer.className = 'toast-container';
        document.body.appendChild(toastContainer);
    }
    
    // Create toast element
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    
    // Icon based on type
    const icons = {
        success: '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M16.667 5L7.5 14.167L3.333 10" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
        error: '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M15 5L5 15M5 5L15 15" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
        warning: '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><path d="M10 6V10M10 14H10.01M18 10C18 14.4183 14.4183 18 10 18C5.58172 18 2 14.4183 2 10C2 5.58172 5.58172 2 10 2C14.4183 2 18 5.58172 18 10Z" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>',
        info: '<svg width="20" height="20" viewBox="0 0 20 20" fill="none"><circle cx="10" cy="10" r="8" stroke="currentColor" stroke-width="2"/><path d="M10 10V14M10 6H10.01" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>'
    };
    
    toast.innerHTML = `
        <div class="toast-icon">${icons[type] || icons.info}</div>
        <div class="toast-message">${escapeHtml(message)}</div>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;
    
    // Add to container
    toastContainer.appendChild(toast);
    
    // Animate in
    setTimeout(() => toast.classList.add('show'), 10);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

// Insert Sample Query
function insertSample(type) {
    const samples = {
        'SELECT': 'SELECT * FROM table_name;',
        'INSERT': 'INSERT INTO table_name (column1, column2) VALUES (value1, value2);',
        'CREATE': 'CREATE TABLE table_name (id INT PRIMARY KEY, name TEXT);',
        'UPDATE': 'UPDATE table_name SET column1 = value1 WHERE condition;',
        'DELETE': 'DELETE FROM table_name WHERE condition;',
        'DESCRIBE': 'DESCRIBE table_name;'
    };
    
    if (samples[type]) {
        sqlEditor.value = samples[type];
        sqlEditor.focus();
        updateEditorInfo();
    }
}

// Show Examples Modal
function showExamples() {
    document.getElementById('examplesModal').classList.add('active');
}

// Close Modal
function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// Use Example
function useExample(element) {
    const code = element.querySelector('code').textContent;
    sqlEditor.value = code;
    updateEditorInfo();
    closeModal('examplesModal');
    sqlEditor.focus();
}

// Toggle History
function toggleHistory() {
    const isVisible = historyPanel.style.display !== 'none';
    historyPanel.style.display = isVisible ? 'none' : 'block';
    
    if (!isVisible) {
        renderHistory();
    }
}

// Render History
function renderHistory() {
    const historyContent = document.getElementById('historyContent');
    
    if (client.queryHistory.length === 0) {
        historyContent.innerHTML = '<div class="empty-state"><p>No query history yet</p></div>';
        return;
    }

    const html = client.queryHistory.map(entry => `
        <div class="history-item" onclick="useHistoryQuery(this)" data-sql="${escapeHtml(entry.sql)}">
            <div class="history-item-header">
                <span class="history-item-time">${formatTimestamp(entry.timestamp)}</span>
                <span class="history-item-status ${entry.success ? 'success' : 'error'}">
                    ${entry.success ? '✓ Success' : '✗ Failed'}
                </span>
            </div>
            <div class="history-item-query">${escapeHtml(entry.sql)}</div>
        </div>
    `).join('');

    historyContent.innerHTML = html;
}

// Use History Query
function useHistoryQuery(element) {
    const sql = element.getAttribute('data-sql');
    sqlEditor.value = sql;
    updateEditorInfo();
    historyPanel.style.display = 'none';
    sqlEditor.focus();
}

// Clear History
function handleClearHistory() {
    if (confirm('Clear all query history?')) {
        client.clearHistory();
        renderHistory();
        showNotification('History cleared', 'success');
    }
}

// Show Help
function showHelp() {
    alert(`Mindb Web Console - Keyboard Shortcuts:

Ctrl/Cmd + Enter: Execute query
Tab: Insert 4 spaces

Tips:
- Use the Examples button to see sample queries
- Query history is automatically saved
- Export results to CSV using the Export button`);
}

// Stored Procedures Functions
function showStoredProcedures() {
    if (!client.connected) {
        showNotification('Please connect to a database first', 'error');
        return;
    }
    
    switchTab('procedures');
}

function showUploadProcedureForm() {
    document.getElementById('uploadProcedureForm').style.display = 'block';
    document.getElementById('procedureName').focus();
}

function hideUploadProcedureForm() {
    document.getElementById('uploadProcedureForm').style.display = 'none';
    // Clear form
    document.getElementById('procedureName').value = '';
    document.getElementById('wasmFile').value = '';
    document.getElementById('procedureDescription').value = '';
}

async function loadStoredProcedures() {
    try {
        const url = client.serverUrl ? `${client.serverUrl}/procedures` : '/procedures';
        const response = await fetch(url, {
            headers: {
                'X-Mindb-Database': client.database
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to load procedures');
        }
        
        const data = await response.json();
        displayProcedures(data.procedures || []);
    } catch (error) {
        showNotification(`Failed to load procedures: ${error.message}`, 'error');
    }
}

function displayProcedures(procedures) {
    const list = document.getElementById('proceduresList');
    
    if (!procedures || procedures.length === 0) {
        list.innerHTML = '<div class="empty-state"><p>No stored procedures yet</p></div>';
        return;
    }
    
    list.innerHTML = procedures.map(proc => {
        // Build function signature
        let signature = `${escapeHtml(proc.name)}(`;
        if (proc.params && proc.params.length > 0) {
            const paramStrs = proc.params.map(p => {
                const paramName = p.name || p.Name || 'param';
                const paramType = p.data_type || p.DataType || 'UNKNOWN';
                return `${paramName}: ${paramType}`;
            });
            signature += paramStrs.join(', ');
        }
        signature += `) → ${escapeHtml(proc.return_type || 'VOID')}`;
        
        return `
        <div style="padding: 1rem; background: var(--bg-secondary); border-radius: 8px; margin-bottom: 0.75rem;">
            <div style="display: flex; justify-content: space-between; align-items: start;">
                <div style="flex: 1;">
                    <div>
                        ${signature}
                    </div>
                    ${proc.description ? `<p style="font-size: 0.875rem; color: var(--text-secondary); margin-bottom: 0.5rem;">${escapeHtml(proc.description)}</p>` : ''}
                </div>
                <button class="btn btn-secondary btn-sm" onclick="deleteProcedure('${escapeHtml(proc.name)}')" style="color: var(--error);">Delete</button>
            </div>
        </div>
    `;
    }).join('');
}

async function handleUploadProcedure() {
    const name = document.getElementById('procedureName').value.trim();
    const fileInput = document.getElementById('wasmFile');
    const description = document.getElementById('procedureDescription').value.trim();
    
    // Validation
    if (!name) {
        showNotification('Please enter a function name', 'error');
        return;
    }
    
    if (!/^[a-zA-Z0-9_]+$/.test(name)) {
        showNotification('Function name can only contain letters, numbers, and underscores', 'error');
        return;
    }
    
    if (!fileInput.files || fileInput.files.length === 0) {
        showNotification('Please select a WASM file', 'error');
        return;
    }
    
    const file = fileInput.files[0];
    if (!file.name.endsWith('.wasm')) {
        showNotification('Please select a valid .wasm file', 'error');
        return;
    }
    
    uploadProcedureBtn.disabled = true;
    uploadProcedureBtn.textContent = 'Uploading...';
    
    try {
        // Read file as ArrayBuffer
        const arrayBuffer = await file.arrayBuffer();
        const bytes = new Uint8Array(arrayBuffer);
        
        // Convert to base64
        let binary = '';
        for (let i = 0; i < bytes.length; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        const wasmBase64 = btoa(binary);
        
        // Build request body - always auto-detect params and return type
        const requestBody = {
            name,
            language: 'wasm',
            wasm_base64: wasmBase64,
            description
        };
        // Note: params and return_type are intentionally omitted to trigger auto-detection
        
        // Upload procedure
        const url = client.serverUrl ? `${client.serverUrl}/procedures` : '/procedures';
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Mindb-Database': client.database
            },
            body: JSON.stringify(requestBody)
        });
        
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error?.message || 'Failed to upload procedure');
        }
        
        showNotification(`Procedure '${name}' uploaded successfully!`, 'success');
        hideUploadProcedureForm();
        loadStoredProcedures();
    } catch (error) {
        showNotification(`Failed to upload procedure: ${error.message}`, 'error');
    } finally {
        uploadProcedureBtn.disabled = false;
        uploadProcedureBtn.textContent = 'Upload Procedure';
    }
}

async function deleteProcedure(name) {
    if (!confirm(`Are you sure you want to delete procedure '${name}'?`)) {
        return;
    }
    
    try {
        const url = client.serverUrl ? `${client.serverUrl}/procedures/${name}` : `/procedures/${name}`;
        const response = await fetch(url, {
            method: 'DELETE',
            headers: {
                'X-Mindb-Database': client.database
            }
        });
        
        if (!response.ok) {
            throw new Error('Failed to delete procedure');
        }
        
        showNotification(`Procedure '${name}' deleted successfully`, 'success');
        loadStoredProcedures();
    } catch (error) {
        showNotification(`Failed to delete procedure: ${error.message}`, 'error');
    }
}

// Load Saved Connection
function loadSavedConnection() {
    const savedUrl = localStorage.getItem('mindb_server_url');
    const savedDb = localStorage.getItem('mindb_database');
    
    if (savedUrl) {
        document.getElementById('serverUrl').value = savedUrl;
    }
    if (savedDb) {
        document.getElementById('database').value = savedDb;
    }
}

// Utility Functions
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

function formatValue(value) {
    if (value === null) return 'NULL';
    if (value === undefined) return '';
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    return String(value);
}

function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    return date.toLocaleString();
}

// Close modal on outside click
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        e.target.classList.remove('active');
    }
});

// Handle Enter key in create database modal
document.getElementById('newDbName').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
        e.preventDefault();
        handleConfirmCreateDb();
    }
});
