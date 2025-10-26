# Mindb Web Console

A modern, beautiful web interface for interacting with Mindb database server.

![Mindb Web Console](screenshot.png)

## Features

✨ **Modern UI**
- Dark theme with beautiful gradients
- Responsive design
- Smooth animations and transitions

🚀 **Powerful Query Editor**
- Syntax highlighting
- Line and character count
- Keyboard shortcuts (Ctrl/Cmd + Enter to execute)
- Tab support for indentation

📊 **Results Display**
- Clean table view for SELECT queries
- Success/error messages
- Export to CSV
- Latency tracking

📝 **Query History**
- Automatic history tracking
- Success/failure indicators
- Click to reuse queries
- Persistent storage (localStorage)

🎯 **Quick Actions**
- SQL examples library
- Sample query templates
- Connection management
- Server status monitoring

---

## Quick Start

### 1. Start Mindb Server

```bash
cd cmd/mindb-server
go run main.go
```

The server will start on `http://localhost:8080`

### 2. Open Web Console

Simply open your browser and navigate to:

```
http://localhost:8080/console
```

The web console is now served directly by the mindb-server (no CORS issues!)

### 3. Connect

1. Leave server URL empty (uses same server)
2. (Optional) Enter database name
3. Click **Connect**

### 4. Execute Queries

```sql
-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT
);

-- Insert data
INSERT INTO users (id, name, email) 
VALUES (1, 'Alice', 'alice@example.com');

-- Query data
SELECT * FROM users;
```

---

## Usage

### Connection

**Server URL**: The HTTP endpoint of your Mindb server (default: `http://localhost:8080`)

**Database**: Optional database name. If not specified, uses the default database.

### Query Editor

**Execute Query**: Click the **Execute** button or press `Ctrl/Cmd + Enter`

**Clear Editor**: Click the **Clear** button

**Insert Sample**: Click quick action buttons (SELECT, INSERT, CREATE, etc.)

### Results

**View Results**: Results appear in a table below the editor

**Export CSV**: Click **Export CSV** to download results

**Latency**: Execution time is displayed in the sidebar

### History

**View History**: Click the **History** button in the sidebar

**Reuse Query**: Click any history item to load it into the editor

**Clear History**: Click **Clear History** to remove all entries

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl/Cmd + Enter` | Execute query |
| `Tab` | Insert 4 spaces |

---

## SQL Examples

The web console includes built-in examples for:

### Table Operations
```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
DROP TABLE users;
```

### Data Manipulation
```sql
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
SELECT * FROM users;
SELECT * FROM users WHERE id = 1;
UPDATE users SET email = 'newemail@example.com' WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

### Transactions
```sql
BEGIN;
-- your queries here
COMMIT;
-- or
ROLLBACK;
```

---

## Features in Detail

### 1. Connection Management

The web console automatically:
- Tests server connectivity
- Saves connection settings
- Displays connection status
- Shows server information

### 2. Query Execution

Supports all Mindb SQL operations:
- **DDL**: CREATE, DROP, ALTER
- **DML**: INSERT, UPDATE, DELETE, SELECT
- **TCL**: BEGIN, COMMIT, ROLLBACK

### 3. Results Display

**SELECT Queries**:
- Displays results in a clean table
- Shows column names
- Handles NULL values
- Supports scrolling for large results

**Non-SELECT Queries**:
- Shows success message
- Displays affected rows
- Shows execution time

**Errors**:
- Clear error messages
- Syntax error highlighting
- Helpful suggestions

### 4. Query History

Automatically tracks:
- All executed queries
- Success/failure status
- Execution time
- Timestamp

History is:
- Stored locally (localStorage)
- Limited to 50 most recent queries
- Persistent across sessions
- Exportable

### 5. Export Functionality

Export results to CSV:
- Includes column headers
- Handles special characters
- Proper CSV escaping
- Downloads directly

---

## Architecture

```
┌─────────────────────────────────────────┐
│         Mindb Web Console               │
│  (HTML + CSS + JavaScript)              │
└─────────────────────────────────────────┘
                  │
                  │ HTTP/JSON
                  ▼
┌─────────────────────────────────────────┐
│         Mindb Server                    │
│  (Go HTTP Server)                       │
└─────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│         Mindb Engine                    │
│  (SQL Database)                         │
└─────────────────────────────────────────┘
```

### Technology Stack

**Frontend**:
- Pure HTML5
- CSS3 (Custom properties, Grid, Flexbox)
- Vanilla JavaScript (ES6+)
- No frameworks or dependencies

**Fonts**:
- Inter (UI)
- JetBrains Mono (Code)

**API**:
- RESTful HTTP
- JSON payloads
- Standard HTTP methods

---

## Configuration

### Server URL

Default: `http://localhost:8080`

To connect to a different server, simply enter the URL in the connection panel.

### Database

Optional. If not specified, queries execute against the default database.

To use a specific database, enter the name in the connection panel.

### Customization

Edit `styles.css` to customize:
- Colors (CSS variables in `:root`)
- Fonts
- Spacing
- Animations

---

## Browser Compatibility

Tested and working on:
- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

Requires:
- ES6+ JavaScript support
- CSS Grid and Flexbox
- Fetch API
- LocalStorage

---

## Troubleshooting

### Connection Failed

**Problem**: Cannot connect to server

**Solutions**:
1. Verify server is running: `curl http://localhost:8080/health`
2. Check server URL is correct
3. Ensure no firewall blocking
4. Check CORS settings if accessing from different domain

### Query Execution Failed

**Problem**: Query returns error

**Solutions**:
1. Check SQL syntax
2. Verify table/column names
3. Check data types
4. Review error message

### Results Not Displaying

**Problem**: Query succeeds but no results shown

**Solutions**:
1. Check if query returns data (SELECT)
2. Verify table has rows
3. Check browser console for errors
4. Try refreshing the page

### History Not Saving

**Problem**: Query history disappears

**Solutions**:
1. Check browser localStorage is enabled
2. Verify not in private/incognito mode
3. Check browser storage quota
4. Try clearing browser cache

---

## Development

### File Structure

```
cmd/mindb-web/
├── index.html      # Main HTML file
├── styles.css      # Stylesheet
├── app.js          # Application logic
├── server.go       # Web server
├── go.mod          # Go module
├── start.sh        # Quick start script
└── README.md       # This file
```

### Local Development

1. Make changes to HTML/CSS/JS files
2. Refresh browser to see changes
3. Use browser DevTools for debugging

### Adding Features

**New SQL Examples**:
Edit the `examplesModal` section in `index.html`

**New Keyboard Shortcuts**:
Edit the `handleEditorKeydown` function in `app.js`

**Styling Changes**:
Edit CSS variables in `styles.css` `:root` section

---

## API Endpoints Used

The web console uses these Mindb server endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Check server status |
| `/execute` | POST | Execute SQL query |

### Request Format

```json
{
  "sql": "SELECT * FROM users;"
}
```

### Response Format

**SELECT Query**:
```json
{
  "columns": ["id", "name", "email"],
  "rows": [
    [1, "Alice", "alice@example.com"],
    [2, "Bob", "bob@example.com"]
  ]
}
```

**Non-SELECT Query**:
```json
{
  "result": "1 row affected"
}
```

**Error**:
```json
{
  "error": {
    "code": "SYNTAX_ERROR",
    "message": "syntax error near 'SELEC'"
  }
}
```

---

## Security Considerations

### Client-Side Only

The web console is a **client-side application**:
- No server-side code
- No authentication built-in
- Direct connection to Mindb server

### Production Use

For production deployment:

1. **Use HTTPS**: Serve over HTTPS
2. **Add Authentication**: Implement auth in Mindb server
3. **CORS**: Configure proper CORS headers
4. **Rate Limiting**: Add rate limiting to server
5. **Input Validation**: Server-side validation

### Local Development

For local development:
- Safe to use as-is
- No external dependencies
- No data sent to third parties

---

## Future Enhancements

Potential features for future versions:

- [ ] Syntax highlighting in editor
- [ ] Auto-completion
- [ ] Query formatting
- [ ] Multiple tabs
- [ ] Saved queries
- [ ] Dark/light theme toggle
- [ ] Query execution plans
- [ ] Database schema browser
- [ ] Visual query builder
- [ ] Real-time query monitoring
- [ ] Collaborative features
- [ ] Mobile app version

---

## Contributing

Contributions welcome! To contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

---

## License

Same license as Mindb project.

---

## Support

For issues or questions:
- Check the troubleshooting section
- Review Mindb server logs
- Open an issue on GitHub

---

## Credits

**Design**: Modern dark theme inspired by popular developer tools

**Fonts**:
- Inter by Rasmus Andersson
- JetBrains Mono by JetBrains

**Icons**: Custom SVG icons

---

**Version**: 1.0.0  
**Status**: ✅ Production Ready  
**Last Updated**: October 5, 2025
