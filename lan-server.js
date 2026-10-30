/**
 * SHIV SONOGRAPHY & DIAGNOSTIC CENTRE
 * Local LAN Real-Time Multi-Computer Synchronization Server (Mode B)
 * 
 * Features:
 * - High-speed WebSocket live broadcasting (<5ms latency across local Wi-Fi/LAN)
 * - REST API endpoints for two-way sync, CRUD operations & atomic bill number sequencing
 * - File-based persistent storage (./data/lan_sync_database.json)
 * - Automatic local IP address discovery for easy client PC connection
 * - Built-in static web server (clients can run the app directly from http://<IP>:3000)
 */

const express = require('express');
const http = require('http');
const cors = require('cors');
const { WebSocketServer, WebSocket } = require('ws');
const fs = require('fs');
const path = require('path');
const os = require('os');

// Configuration
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';
const DATA_DIR = path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'lan_sync_database.json');

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// In-Memory Database Structure
let db = {
  bills: [],
  config: null,
  catalog: null,
  nextBillSequence: 54,
  lastUpdated: new Date().toISOString()
};

// Load existing database from disk if present
function loadDatabase() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const raw = fs.readFileSync(DB_FILE, 'utf-8');
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        db = {
          bills: Array.isArray(parsed.bills) ? parsed.bills : [],
          deletedBillIds: Array.isArray(parsed.deletedBillIds) ? parsed.deletedBillIds : [],
          config: parsed.config || null,
          catalog: Array.isArray(parsed.catalog) ? parsed.catalog : null,
          nextBillSequence: parsed.nextBillSequence || 54,
          lastUpdated: parsed.lastUpdated || new Date().toISOString()
        };
        console.log(`[DB] Loaded ${db.bills.length} bills (${db.deletedBillIds.length} deleted records tracked) from ${DB_FILE}`);
      }
    } else {
      db.deletedBillIds = [];
      saveDatabase();
      console.log(`[DB] Initialized new database at ${DB_FILE}`);
    }
  } catch (err) {
    console.error(`[DB ERROR] Error reading database: ${err.message}`);
  }
}

// Save database to disk with atomic write
let saveTimeout = null;
function scheduleSaveDatabase() {
  if (saveTimeout) clearTimeout(saveTimeout);
  saveTimeout = setTimeout(saveDatabase, 300); // 300ms debounce
}

function saveDatabase() {
  try {
    db.lastUpdated = new Date().toISOString();
    const tempFile = DB_FILE + '.tmp';
    fs.writeFileSync(tempFile, JSON.stringify(db, null, 2), 'utf-8');
    fs.renameSync(tempFile, DB_FILE);
  } catch (err) {
    console.error(`[DB ERROR] Failed to save database: ${err.message}`);
  }
}

// Initialize database
loadDatabase();

// Setup Express App
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '25mb' }));

// Serve static web app files directly so client PCs can open http://<IP>:3000
app.use(express.static(__dirname));

// Create HTTP server
const server = http.createServer(app);

// Create WebSocket Server
const wss = new WebSocketServer({ server, path: '/ws' });

// Track connected workstations
const connectedClients = new Map(); // ws -> { workstationId, workstationName, ip, connectedAt }

// Broadcast message to all connected clients except sender
function broadcast(message, senderWs = null) {
  const payload = typeof message === 'string' ? message : JSON.stringify(message);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client !== senderWs) {
      client.send(payload);
    }
  });
}

// Get list of active workstations
function getActiveWorkstations() {
  const list = [];
  connectedClients.forEach(info => {
    list.push({
      workstationId: info.workstationId,
      workstationName: info.workstationName,
      ip: info.ip,
      connectedAt: info.connectedAt
    });
  });
  return list;
}

// WebSocket Event Handling
wss.on('connection', (ws, req) => {
  const remoteIp = req.socket.remoteAddress || 'unknown';
  const clientInfo = {
    workstationId: 'PC-' + Math.floor(100 + Math.random() * 900),
    workstationName: 'Workstation',
    ip: remoteIp,
    connectedAt: new Date().toISOString()
  };
  connectedClients.set(ws, clientInfo);

  console.log(`[WS CONNECT] Client connected from ${remoteIp}. Total: ${wss.clients.size}`);

  // Send initial welcome & peer list
  ws.send(JSON.stringify({
    type: 'CONNECTED',
    message: 'Connected to Shiv Sonography LAN Sync Server',
    serverTime: new Date().toISOString(),
    totalBills: db.bills.length,
    activeWorkstations: getActiveWorkstations()
  }));

  ws.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());
      handleWebSocketMessage(ws, msg);
    } catch (err) {
      console.error('[WS ERROR] Malformed message:', err.message);
    }
  });

  ws.on('close', () => {
    const info = connectedClients.get(ws);
    connectedClients.delete(ws);
    console.log(`[WS DISCONNECT] ${info ? info.workstationId : 'Client'} disconnected. Remaining: ${wss.clients.size}`);
    
    // Broadcast peer leave
    if (info) {
      broadcast({
        type: 'PEER_LEFT',
        workstationId: info.workstationId,
        workstationName: info.workstationName,
        activeWorkstations: getActiveWorkstations()
      });
    }
  });

  ws.on('error', err => {
    console.error(`[WS SOCKET ERROR]`, err.message);
  });
});

// Handle WebSocket action messages
function handleWebSocketMessage(ws, msg) {
  const clientInfo = connectedClients.get(ws);

  switch (msg.type) {
    case 'HANDSHAKE': {
      if (clientInfo && msg.workstationId) {
        clientInfo.workstationId = msg.workstationId;
        clientInfo.workstationName = msg.workstationName || msg.workstationId;
      }
      console.log(`[HANDSHAKE] Workstation identified: ${clientInfo.workstationId} (${clientInfo.workstationName})`);
      
      ws.send(JSON.stringify({
        type: 'HANDSHAKE_ACK',
        workstationId: clientInfo.workstationId,
        serverTime: new Date().toISOString(),
        totalBills: db.bills.length,
        activeWorkstations: getActiveWorkstations()
      }));

      // Broadcast to other peers
      broadcast({
        type: 'PEER_JOINED',
        workstationId: clientInfo.workstationId,
        workstationName: clientInfo.workstationName,
        activeWorkstations: getActiveWorkstations()
      }, ws);
      break;
    }

    case 'PING': {
      ws.send(JSON.stringify({ type: 'PONG', timestamp: Date.now() }));
      break;
    }

    case 'BILL_UPDATED': {
      const bill = msg.bill;
      if (!bill || !bill.id) return;

      // Ensure deleted records can never be restored
      if (db.deletedBillIds && (db.deletedBillIds.includes(bill.id) || (bill.billNo && db.deletedBillIds.includes(bill.billNo)))) {
        console.warn(`[SYNC REJECTED] Workstation ${msg.workstationId} attempted to restore permanently deleted Bill #${bill.billNo} (${bill.id}). Action blocked.`);
        return;
      }

      bill.updatedAt = bill.updatedAt || new Date().toISOString();
      bill.workstationId = msg.workstationId || (clientInfo ? clientInfo.workstationId : 'Unknown');

      // Upsert in database
      const existingIdx = db.bills.findIndex(b => b.id === bill.id);
      if (existingIdx >= 0) {
        db.bills[existingIdx] = bill;
      } else {
        db.bills.unshift(bill);
      }
      scheduleSaveDatabase();

      console.log(`[SYNC BROADCAST] Bill #${bill.billNo} updated by ${bill.workstationId} (${bill.patientName})`);

      // Broadcast to all other workstations immediately
      broadcast({
        type: 'BILL_UPDATED',
        bill: bill,
        workstationId: bill.workstationId,
        workstationName: clientInfo ? clientInfo.workstationName : bill.workstationId,
        timestamp: new Date().toISOString()
      }, ws);
      break;
    }

    case 'BILL_DELETED': {
      const billId = msg.billId;
      if (!billId) return;

      if (!db.deletedBillIds) db.deletedBillIds = [];
      if (!db.deletedBillIds.includes(billId)) {
        db.deletedBillIds.push(billId);
      }

      const idx = db.bills.findIndex(b => b.id === billId || b.billNo === billId);
      let deletedBillNo = '';
      if (idx >= 0) {
        deletedBillNo = db.bills[idx].billNo;
        if (deletedBillNo && !db.deletedBillIds.includes(deletedBillNo)) {
          db.deletedBillIds.push(deletedBillNo);
        }
        db.bills.splice(idx, 1);
      }
      scheduleSaveDatabase();

      console.log(`[SYNC BROADCAST] Bill ${billId} (${deletedBillNo}) permanently deleted by ${msg.workstationId}`);

      broadcast({
        type: 'BILL_DELETED',
        billId: billId,
        billNo: deletedBillNo,
        workstationId: msg.workstationId,
        workstationName: clientInfo ? clientInfo.workstationName : msg.workstationId,
        timestamp: new Date().toISOString()
      }, ws);
      break;
    }

    case 'CLEAR_ALL_BILLS': {
      if (!db.deletedBillIds) db.deletedBillIds = [];
      db.bills.forEach(b => {
        if (b && b.id && !db.deletedBillIds.includes(b.id)) db.deletedBillIds.push(b.id);
        if (b && b.billNo && !db.deletedBillIds.includes(b.billNo)) db.deletedBillIds.push(b.billNo);
      });
      const clearedCount = db.bills.length;
      db.bills = [];
      scheduleSaveDatabase();

      console.log(`[SYNC BROADCAST] All ${clearedCount} bills permanently deleted by ${msg.workstationId}`);

      broadcast({
        type: 'ALL_BILLS_CLEARED',
        workstationId: msg.workstationId,
        workstationName: clientInfo ? clientInfo.workstationName : msg.workstationId,
        timestamp: new Date().toISOString()
      }, ws);
      break;
    }

    case 'CONFIG_UPDATED': {
      if (msg.config) {
        db.config = msg.config;
        scheduleSaveDatabase();
        broadcast({
          type: 'CONFIG_UPDATED',
          config: db.config,
          workstationId: msg.workstationId
        }, ws);
      }
      break;
    }

    case 'REQUEST_FULL_SYNC': {
      ws.send(JSON.stringify({
        type: 'FULL_SYNC_DATA',
        bills: db.bills,
        deletedBillIds: db.deletedBillIds || [],
        config: db.config,
        serverTime: new Date().toISOString()
      }));
      break;
    }

    default:
      console.log('[WS] Unknown message type:', msg.type);
  }
}

// ================= REST API ROUTES =================

// Health & Status
app.get('/api/health', (req, res) => {
  res.json({
    status: 'online',
    centerName: (db.config && db.config.centerName) || "SHIV SONOGRAPHY & DIAGNOSTIC CENTRE",
    serverTime: new Date().toISOString(),
    connectedCount: wss.clients.size,
    activeWorkstations: getActiveWorkstations(),
    totalBills: db.bills.length,
    port: PORT,
    localIps: getLocalIpAddresses()
  });
});

app.get('/api/status', (req, res) => {
  res.json({
    status: 'online',
    serverTime: new Date().toISOString(),
    totalBills: db.bills.length,
    connectedCount: wss.clients.size,
    activeWorkstations: getActiveWorkstations(),
    lastUpdated: db.lastUpdated
  });
});

// Fetch all bills
app.get('/api/bills', (req, res) => {
  res.json({
    success: true,
    count: db.bills.length,
    bills: db.bills
  });
});

// Atomic Sequential Bill Number Generator
// Multi-computer conflict prevention: Guarantees unique voucher numbers across all computers
app.get('/api/next-bill-number', (req, res) => {
  const prefix = req.query.prefix || (db.config && db.config.billPrefix) || "NWR/2026-27/";
  let maxNum = 0;

  db.bills.forEach(item => {
    if (item.billNo && item.billNo.startsWith(prefix)) {
      const numPart = item.billNo.replace(prefix, '').trim();
      const parsed = parseInt(numPart, 10);
      if (!isNaN(parsed) && parsed > maxNum) {
        maxNum = parsed;
      }
    }
  });

  const nextSeq = Math.max(maxNum + 1, (db.nextBillSequence || 54));
  db.nextBillSequence = nextSeq + 1;
  scheduleSaveDatabase();

  const formattedSeq = String(nextSeq).padStart(5, '0');
  const billNo = `${prefix}${formattedSeq}`;

  res.json({
    success: true,
    billNo: billNo,
    sequence: nextSeq,
    prefix: prefix
  });
});

// Save or Update Single Bill
app.post('/api/bills', (req, res) => {
  const bill = req.body.bill || req.body;
  const workstationId = req.body.workstationId || 'REST-Client';

  if (!bill || !bill.id) {
    return res.status(400).json({ success: false, error: 'Bill ID is required' });
  }

  // Prevent restoration of permanently deleted bill
  if (db.deletedBillIds && (db.deletedBillIds.includes(bill.id) || (bill.billNo && db.deletedBillIds.includes(bill.billNo)))) {
    return res.status(409).json({ success: false, error: 'Bill has been permanently deleted and cannot be restored.' });
  }

  bill.updatedAt = bill.updatedAt || new Date().toISOString();
  bill.workstationId = workstationId;

  const existingIdx = db.bills.findIndex(b => b.id === bill.id);
  if (existingIdx >= 0) {
    db.bills[existingIdx] = bill;
  } else {
    db.bills.unshift(bill);
  }
  scheduleSaveDatabase();

  // Broadcast to WebSockets
  broadcast({
    type: 'BILL_UPDATED',
    bill: bill,
    workstationId: workstationId,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, bill: bill });
});

// Delete Single Bill (Permanent Delete)
app.delete('/api/bills/:id', (req, res) => {
  const billId = req.params.id;
  const workstationId = req.query.workstationId || 'REST-Client';

  if (!db.deletedBillIds) db.deletedBillIds = [];
  if (!db.deletedBillIds.includes(billId)) {
    db.deletedBillIds.push(billId);
  }

  const idx = db.bills.findIndex(b => b.id === billId || b.billNo === billId);
  let deletedBillNo = '';
  if (idx >= 0) {
    const deleted = db.bills.splice(idx, 1)[0];
    deletedBillNo = deleted.billNo;
    if (deletedBillNo && !db.deletedBillIds.includes(deletedBillNo)) {
      db.deletedBillIds.push(deletedBillNo);
    }
  }
  scheduleSaveDatabase();

  broadcast({
    type: 'BILL_DELETED',
    billId: billId,
    billNo: deletedBillNo,
    workstationId: workstationId,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, deletedId: billId, billNo: deletedBillNo });
});

// Clear All Bills (Permanent Bulk Deletion)
app.delete('/api/bills', (req, res) => {
  const workstationId = req.query.workstationId || 'REST-Client';
  if (!db.deletedBillIds) db.deletedBillIds = [];
  db.bills.forEach(b => {
    if (b && b.id && !db.deletedBillIds.includes(b.id)) db.deletedBillIds.push(b.id);
    if (b && b.billNo && !db.deletedBillIds.includes(b.billNo)) db.deletedBillIds.push(b.billNo);
  });
  const count = db.bills.length;
  db.bills = [];
  scheduleSaveDatabase();

  broadcast({
    type: 'ALL_BILLS_CLEARED',
    workstationId: workstationId,
    timestamp: new Date().toISOString()
  });

  res.json({ success: true, clearedCount: count });
});

// Two-Way Reconciliation & Merge Endpoint
// Used when client connects, switches modes, or clicks "Force Sync All Records"
app.post('/api/sync', (req, res) => {
  const clientBills = Array.isArray(req.body.bills) ? req.body.bills : [];
  const workstationId = req.body.workstationId || 'Client';

  if (!db.deletedBillIds) db.deletedBillIds = [];

  let addedToServer = 0;
  let updatedOnServer = 0;

  const serverBillMap = new Map();
  db.bills.forEach((b, idx) => serverBillMap.set(b.id, idx));

  clientBills.forEach(cBill => {
    if (!cBill || !cBill.id) return;
    
    // Discard any incoming bills that were permanently deleted
    if (db.deletedBillIds.includes(cBill.id) || (cBill.billNo && db.deletedBillIds.includes(cBill.billNo))) {
      return;
    }

    if (serverBillMap.has(cBill.id)) {
      const sIdx = serverBillMap.get(cBill.id);
      const sBill = db.bills[sIdx];
      const sTime = new Date(sBill.updatedAt || sBill.createdAt || 0).getTime();
      const cTime = new Date(cBill.updatedAt || cBill.createdAt || 0).getTime();

      // If client copy is newer, update server copy
      if (cTime > sTime) {
        db.bills[sIdx] = cBill;
        updatedOnServer++;
      }
    } else {
      // New bill from client that server doesn't have
      db.bills.unshift(cBill);
      serverBillMap.set(cBill.id, 0);
      addedToServer++;
    }
  });

  if (addedToServer > 0 || updatedOnServer > 0) {
    scheduleSaveDatabase();
    // Notify other connected clients of the batch sync additions
    broadcast({
      type: 'BATCH_SYNC_OCCURRED',
      workstationId: workstationId,
      serverBillCount: db.bills.length,
      timestamp: new Date().toISOString()
    });
  }

  // Return server's complete master list of bills back to the client along with deleted IDs
  res.json({
    success: true,
    bills: db.bills,
    deletedBillIds: db.deletedBillIds || [],
    config: db.config,
    serverTime: new Date().toISOString(),
    stats: {
      addedToServer,
      updatedOnServer,
      totalServerBills: db.bills.length
    }
  });
});

// Config endpoints
app.get('/api/config', (req, res) => {
  res.json({ success: true, config: db.config });
});

app.post('/api/config', (req, res) => {
  if (req.body && req.body.config) {
    db.config = req.body.config;
    scheduleSaveDatabase();
    broadcast({
      type: 'CONFIG_UPDATED',
      config: db.config,
      workstationId: req.body.workstationId || 'Client'
    });
    res.json({ success: true });
  } else {
    res.status(400).json({ error: 'Config required' });
  }
});

// Full Backup Export/Import
app.get('/api/backup', (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Content-Disposition', `attachment; filename=shiv_lan_backup_${Date.now()}.json`);
  res.send(JSON.stringify(db, null, 2));
});

// Utility: Discover Local IPv4 Addresses
function getLocalIpAddresses() {
  const interfaces = os.networkInterfaces();
  const addresses = [];

  for (const name of Object.keys(interfaces)) {
    for (const net of interfaces[name]) {
      // Skip over non-IPv4 and internal (i.e. 127.0.0.1) addresses
      if (net.family === 'IPv4' && !net.internal) {
        addresses.push(net.address);
      }
    }
  }
  return addresses;
}

// Start Server
server.listen(PORT, HOST, () => {
  const localIps = getLocalIpAddresses();
  const primaryIp = localIps.length > 0 ? localIps[0] : '127.0.0.1';

  console.log(`
╔══════════════════════════════════════════════════════════════════════════════════╗
║              SHIV SONOGRAPHY & DIAGNOSTIC CENTRE - PHULERA                       ║
║             LOCAL LAN REAL-TIME SYNC SERVER (MODE B - ACTIVE)                   ║
║         Multi-Computer Sync for Patient Registrations, Billing & Claims         ║
╚══════════════════════════════════════════════════════════════════════════════════╝

 [STATUS] Live LAN Sync Server running on port ${PORT}
 [STORAGE] Database File: ${DB_FILE} (${db.bills.length} bills loaded)

 >>> CONNECT OTHER COMPUTERS ON YOUR CLINIC WI-FI / LAN:
     Open any web browser (Chrome / Edge / Firefox) and visit:
     • Primary LAN URL:   http://${primaryIp}:${PORT}
${localIps.slice(1).map(ip => `     • Alternative IP:    http://${ip}:${PORT}\n`).join('')}     • On this Host PC:   http://localhost:${PORT}

 >>> WEBSOCKET REAL-TIME ENDPOINT:
     ws://${primaryIp}:${PORT}/ws

 >>> CONFLICT PREVENTION & ATOMIC BILL NUMBERING: ACTIVE
     Multi-workstation simultaneous entries will automatically be assigned
     unique, sequential NWR Voucher Numbers with zero overwrite risk.

 [TIP] Press Ctrl+C at any time in this window to stop the server.
════════════════════════════════════════════════════════════════════════════════════
`);
});
