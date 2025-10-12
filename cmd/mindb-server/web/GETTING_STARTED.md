# Getting Started with Mindb Web Console

## 🚀 Quick Start (2 Steps)

### Step 1: Start Mindb Server

```bash
cd cmd/mindb-server
go run main.go
```

Server will start on `http://localhost:8080`

### Step 2: Open Browser

Navigate to: **http://localhost:8080/console**

The web console is served directly by the mindb-server (no CORS issues!)

---

## 📖 Usage

### 1. Connect to Server

1. Leave Server URL empty (uses same server, no CORS issues!)
2. (Optional) Enter Database name
3. Click **Connect**

### 2. Execute Your First Query

```sql
-- Create a table
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');

-- Query data
SELECT * FROM users;
```

### 3. Explore Features

- **SQL Examples**: Click the "SQL Examples" button
- **Query History**: Click the "History" button
- **Export Results**: Click "Export CSV"
- **Keyboard Shortcuts**: Press `Ctrl/Cmd + Enter` to execute

---

## ⚡ Features

✨ **Beautiful UI**
- Modern dark theme
- Responsive design
- Smooth animations

🎯 **Powerful Editor**
- Syntax highlighting
- Line/character count
- Keyboard shortcuts
- Tab support

📊 **Results Display**
- Clean table view
- Export to CSV
- Latency tracking
- Error messages

📝 **Query History**
- Automatic tracking
- Success/failure indicators
- Persistent storage
- Click to reuse

---

## 🎨 Screenshots

### Connection Panel
Connect to your Mindb server with optional database selection.

### Query Editor
Write and execute SQL queries with a powerful editor.

### Results Display
View query results in a beautiful table format.

### Query History
Track all your queries with timestamps and status.

---

## 🔧 Configuration

### Change Port

```bash
./start.sh -port 8000
```

Or:
```bash
go run server.go -port 8000
```

### Connect to Different Server

Simply enter the server URL in the connection panel:
- Local: `http://localhost:8080`
- Remote: `http://your-server:8080`

---

## 📚 Examples

### Create Database

```sql
CREATE DATABASE mydb;
```

### Create Table

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name TEXT,
    price FLOAT,
    stock INT
);
```

### Insert Data

```sql
INSERT INTO products VALUES 
    (1, 'Laptop', 999.99, 10),
    (2, 'Mouse', 29.99, 50),
    (3, 'Keyboard', 79.99, 30);
```

### Query Data

```sql
-- All products
SELECT * FROM products;

-- Products under $100
SELECT * FROM products WHERE price < 100;

-- Count products
SELECT COUNT(*) FROM products;

-- Average price
SELECT AVG(price) FROM products;
```

### Update Data

```sql
UPDATE products 
SET stock = stock - 1 
WHERE id = 1;
```

### Delete Data

```sql
DELETE FROM products 
WHERE stock = 0;
```

### Transactions

```sql
BEGIN;
UPDATE products SET stock = stock - 1 WHERE id = 1;
UPDATE products SET stock = stock + 1 WHERE id = 2;
COMMIT;
```

---

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl/Cmd + Enter` | Execute query |
| `Tab` | Insert 4 spaces |

---

## 🐛 Troubleshooting

### Cannot Connect

**Problem**: Connection failed

**Solution**:
1. Verify Mindb server is running
2. Check server URL is correct
3. Test with: `curl http://localhost:8080/health`

### Query Failed

**Problem**: Query returns error

**Solution**:
1. Check SQL syntax
2. Verify table exists
3. Check data types
4. Review error message

### Port Already in Use

**Problem**: Port 3000 is already in use

**Solution**:
```bash
./start.sh -port 3001
```

---

## 🌐 Browser Support

- ✅ Chrome 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Edge 90+

---

## 📁 Project Structure

```
cmd/mindb-server/
├── main.go             # Server with web console
├── web/                # Web console files
│   ├── index.html      # Main HTML file
│   ├── styles.css      # Stylesheet
│   ├── app.js          # Application logic
│   ├── README.md       # Full documentation
│   └── GETTING_STARTED.md  # This file
└── internal/           # Server internals
```

---

## 🎯 Next Steps

1. **Explore Examples**: Click "SQL Examples" button
2. **Try Transactions**: Use BEGIN/COMMIT/ROLLBACK
3. **Export Data**: Use "Export CSV" button
4. **Check History**: View your query history
5. **Read Docs**: See README.md for full documentation

---

## 💡 Tips

- Use `Ctrl/Cmd + Enter` to quickly execute queries
- Click history items to reuse queries
- Export results to CSV for analysis
- Use the quick action buttons for common queries
- Connection settings are saved automatically

---

## 🆘 Need Help?

- **Documentation**: See README.md
- **Server Logs**: Check Mindb server terminal
- **Browser Console**: Press F12 for developer tools
- **GitHub Issues**: Report bugs or request features

---

## ✨ Features Coming Soon

- Syntax highlighting
- Auto-completion
- Query formatting
- Multiple tabs
- Saved queries
- Schema browser

---

**Status**: ✅ Ready to Use  
**Version**: 1.0.0  
**Last Updated**: October 5, 2025

🎉 **Happy Querying!** 🎉
