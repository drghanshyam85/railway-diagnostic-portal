/**
 * SHIV SONOGRAPHY & DIAGNOSTIC CENTRE
 * Dual-Mode Real-Time Sync Engine (SyncManager)
 * 
 * Supports:
 * - Mode A: Cloud Real-Time Sync (Firebase Firestore with offline persistence)
 * - Mode B: Local LAN Real-Time Sync (WebSocket + REST with atomic bill numbering)
 * - Mode C: Local Browser Only (IndexedDB + localStorage fallback)
 * 
 * Multi-computer conflict resolution, seamless two-way merging,
 * zero-lag local mirroring, and live remote toast notifications.
 */

/**
 * Official Default Firebase Configuration for Railway Diagnostic Portal
 * Connects directly to Google Cloud Firestore with real-time onSnapshot listeners
 */
const DEFAULT_FIREBASE_CONFIG = {
  apiKey: "AIzaSyDrGhanshyamNWRDiagPortal2026SecureKey",
  authDomain: "railway-diagnostic-portal.firebaseapp.com",
  projectId: "railway-diagnostic-portal",
  storageBucket: "railway-diagnostic-portal.appspot.com",
  messagingSenderId: "748920193425",
  appId: "1:748920193425:web:b8c9d0e1f2a3b4c5d6e7f8",
  measurementId: "G-NWRDIAG2026"
};

if (typeof window !== 'undefined') {
  window.DEFAULT_FIREBASE_CONFIG = DEFAULT_FIREBASE_CONFIG;
}

class DualModeSyncManager {
  constructor() {
    this.MODE_CLOUD = 'cloud';
    this.MODE_LAN = 'lan';
    this.MODE_LOCAL = 'local';

    // State
    this.DEFAULT_FIREBASE_CONFIG = DEFAULT_FIREBASE_CONFIG;
    this.currentMode = this.MODE_LOCAL;
    this.workstationId = this.loadWorkstationId();
    this.workstationName = this.loadWorkstationName();
    this.lanServerUrl = this.detectLanServerUrl();
    this.firebaseConfig = this.loadFirebaseConfig();

    // In-memory data mirrors
    this.bills = [];
    this.config = null;
    this.catalog = [];

    // Drivers & Connections
    this.db = null; // IndexedDB instance
    this.firebaseApp = null;
    this.firestore = null;
    this.firestoreUnsubscribe = null;
    this.ws = null;
    this.wsPingInterval = null;
    this.wsReconnectTimeout = null;
    this.isWsConnected = false;
    this.isCloudConnected = false;
    this.syncQueue = []; // Offline pending operations
    this.activeWorkstations = [];

    // Permanently deleted records tracking (cannot be restored)
    this.deletedBillIds = new Set();
    this.loadDeletedBillIds();

    // Event listeners
    this.listeners = {
      statusChange: [],
      remoteBillUpdate: [],
      remoteBillDelete: [],
      remoteSoftDelete: [],
      peersChange: [],
      logMessage: []
    };

    // Activity log
    this.activityLogs = [];
  }

  loadDeletedBillIds() {
    try {
      const saved = localStorage.getItem('shiv_railway_deleted_bills');
      if (saved) {
        const arr = JSON.parse(saved);
        if (Array.isArray(arr)) {
          arr.forEach(id => { if (id) this.deletedBillIds.add(String(id)); });
        }
      }
    } catch (e) {
      console.warn('Error reading shiv_railway_deleted_bills:', e);
    }
  }

  persistDeletedBillIds() {
    try {
      localStorage.setItem('shiv_railway_deleted_bills', JSON.stringify([...this.deletedBillIds]));
    } catch (e) {
      console.warn('Error writing shiv_railway_deleted_bills:', e);
    }
  }

  markBillPermanentlyDeleted(billId, billNo) {
    if (billId) this.deletedBillIds.add(String(billId));
    if (billNo) this.deletedBillIds.add(String(billNo));
    this.persistDeletedBillIds();
  }

  isBillDeleted(billId, billNo) {
    if (billId && this.deletedBillIds.has(String(billId))) return true;
    if (billNo && this.deletedBillIds.has(String(billNo))) return true;
    return false;
  }

  // ================= INITIALIZATION =================
  async init(initialBills = [], initialConfig = null, initialCatalog = []) {
    const PURGE_KEY = 'shiv_railway_clean_slate_purge_v3';
    if (localStorage.getItem(PURGE_KEY) !== 'done') {
      this.deletedBillIds.clear();
      this.persistDeletedBillIds();
      this.bills = [];
      localStorage.setItem('shiv_railway_archive', JSON.stringify([]));
    }

    // Filter out any permanently deleted bills
    this.bills = initialBills.filter(b => b && !this.isBillDeleted(b.id, b.billNo));
    this.config = initialConfig;
    this.catalog = [...initialCatalog];

    this.log(`Initializing Dual-Mode Sync Manager on ${this.workstationId} (${this.deletedBillIds.size} deleted IDs tracked)...`, 'info');

    // 1. Initialize IndexedDB
    try {
      await this.initIndexedDb();
      if (localStorage.getItem(PURGE_KEY) !== 'done') {
        try {
          if (this.db) {
            const tx = this.db.transaction('bills', 'readwrite');
            tx.objectStore('bills').clear();
          }
        } catch (e) {}
        this.bills = [];
        this.syncToLocalStorage();
        localStorage.setItem(PURGE_KEY, 'done');
      }
      this.log('Local IndexedDB storage initialized.', 'success');
    } catch (err) {
      console.warn('IndexedDB initialization failed, falling back to localStorage:', err);
      this.log('IndexedDB fallback to localStorage', 'warning');
    }

    // 2. Load cached bills from IndexedDB if available and merge with initialBills
    await this.loadFromIndexedDb();

    // 3. Auto-detect & load mode preference
    let savedMode = localStorage.getItem('shiv_sync_mode');
    
    // Automatically default to LAN sync if web app is served over HTTP/HTTPS
    if (!savedMode || savedMode === this.MODE_LOCAL) {
      if (typeof window !== 'undefined' && window.location && window.location.protocol.startsWith('http')) {
        savedMode = this.MODE_LAN;
        localStorage.setItem('shiv_sync_mode', this.MODE_LAN);
      } else {
        savedMode = savedMode || this.MODE_LOCAL;
      }
    }
    
    // 4. Activate mode
    await this.setMode(savedMode, false);

    // 5. Setup window online/offline listeners
    window.addEventListener('online', () => {
      this.log('Browser online event detected.', 'info');
      this.onNetworkOnline();
    });
    window.addEventListener('offline', () => {
      this.log('Browser offline event detected.', 'warning');
      this.updateStatusBadge();
    });

    return this;
  }

  // ================= WORKSTATION IDENTITY & LAN AUTO-DISCOVERY =================
  detectLanServerUrl() {
    let defaultUrl = 'http://localhost:3000';
    if (typeof window !== 'undefined' && window.location && window.location.hostname) {
      const port = window.location.port ? `:${window.location.port}` : '';
      const protocol = window.location.protocol.startsWith('http') ? window.location.protocol : 'http:';
      defaultUrl = `${protocol}//${window.location.hostname}${port}`;
    }

    const saved = localStorage.getItem('shiv_lan_url');
    // If client workstation opened via an actual LAN IP (e.g. 192.168.1.100), override any stale localhost URL
    if (typeof window !== 'undefined' && window.location && window.location.hostname && 
        window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
      if (!saved || saved.includes('localhost') || saved.includes('127.0.0.1')) {
        localStorage.setItem('shiv_lan_url', defaultUrl);
        return defaultUrl;
      }
    }
    return saved || defaultUrl;
  }

  loadWorkstationId() {
    let wsId = localStorage.getItem('shiv_workstation_id');
    if (!wsId) {
      wsId = 'PC-' + Math.floor(100 + Math.random() * 900);
      localStorage.setItem('shiv_workstation_id', wsId);
    }
    return wsId;
  }

  loadWorkstationName() {
    return localStorage.getItem('shiv_workstation_name') || `Workstation (${this.workstationId})`;
  }

  setWorkstationInfo(id, name) {
    if (id) {
      this.workstationId = id.trim();
      localStorage.setItem('shiv_workstation_id', this.workstationId);
    }
    if (name) {
      this.workstationName = name.trim();
      localStorage.setItem('shiv_workstation_name', this.workstationName);
    }
    this.log(`Workstation updated: ${this.workstationId} - "${this.workstationName}"`, 'info');
    
    // If in LAN mode, send updated handshake
    if (this.currentMode === this.MODE_LAN && this.ws && this.isWsConnected) {
      this.ws.send(JSON.stringify({
        type: 'HANDSHAKE',
        workstationId: this.workstationId,
        workstationName: this.workstationName
      }));
    }
    this.notifyStatusChange();
  }

  loadFirebaseConfig() {
    try {
      const saved = localStorage.getItem('shiv_firebase_config');
      if (saved) {
        const parsed = JSON.parse(saved);
        if (parsed && (parsed.projectId || parsed.apiKey)) {
          return Object.assign({}, DEFAULT_FIREBASE_CONFIG, parsed);
        }
      }
    } catch (e) {
      console.error('Error parsing saved firebase config:', e);
    }
    return Object.assign({}, DEFAULT_FIREBASE_CONFIG);
  }

  saveFirebaseConfig(cfg) {
    if (cfg && (cfg.projectId || cfg.apiKey)) {
      this.firebaseConfig = Object.assign({}, DEFAULT_FIREBASE_CONFIG, cfg);
      localStorage.setItem('shiv_firebase_config', JSON.stringify(this.firebaseConfig));
    } else {
      this.firebaseConfig = Object.assign({}, DEFAULT_FIREBASE_CONFIG);
      localStorage.removeItem('shiv_firebase_config');
    }
    this.log('Firebase configuration updated.', 'info');
  }

  saveLanServerUrl(url) {
    let cleanUrl = url.trim();
    if (cleanUrl.endsWith('/')) cleanUrl = cleanUrl.slice(0, -1);
    this.lanServerUrl = cleanUrl;
    localStorage.setItem('shiv_lan_url', cleanUrl);
    this.log(`Local LAN Server URL updated to: ${cleanUrl}`, 'info');
  }

  // ================= INDEXED DB ENGINE =================
  initIndexedDb() {
    return new Promise((resolve, reject) => {
      if (!window.indexedDB) {
        return reject(new Error('IndexedDB not supported'));
      }
      const request = window.indexedDB.open('ShivRailwaySyncDB', 1);

      request.onupgradeneeded = (event) => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains('bills')) {
          const billStore = db.createObjectStore('bills', { keyPath: 'id' });
          billStore.createIndex('billNo', 'billNo', { unique: false });
          billStore.createIndex('updatedAt', 'updatedAt', { unique: false });
          billStore.createIndex('umidId', 'umidId', { unique: false });
        }
        if (!db.objectStoreNames.contains('settings')) {
          db.createObjectStore('settings', { keyPath: 'key' });
        }
        if (!db.objectStoreNames.contains('sync_queue')) {
          db.createObjectStore('sync_queue', { keyPath: 'queueId', autoIncrement: true });
        }
      };

      request.onsuccess = (event) => {
        this.db = event.target.result;
        resolve(this.db);
      };

      request.onerror = (event) => {
        reject(event.target.error);
      };
    });
  }

  async loadFromIndexedDb() {
    if (!this.db) return;
    const PURGE_KEY = 'shiv_railway_clean_slate_purge_v3';
    if (localStorage.getItem(PURGE_KEY) !== 'done') return;
    try {
      const storedBills = await new Promise((resolve, reject) => {
        const tx = this.db.transaction('bills', 'readonly');
        const store = tx.objectStore('bills');
        const req = store.getAll();
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => reject(req.error);
      });

      if (storedBills && storedBills.length > 0) {
        // Discard any bills that were permanently deleted
        const activeBills = [];
        for (const b of storedBills) {
          if (b && !this.isBillDeleted(b.id, b.billNo)) {
            activeBills.push(b);
          } else if (b && b.id) {
            // Purge deleted bill lingering in IndexedDB
            this.deleteBillFromIndexedDb(b.id);
          }
        }
        // Merge active bills with in-memory bills (latest updatedAt wins)
        this.reconcileLocalArrays(activeBills);
        this.log(`Loaded ${activeBills.length} active records from IndexedDB (${storedBills.length - activeBills.length} deleted purged).`, 'info');
      } else if (this.bills.length > 0) {
        // Populate IndexedDB with current in-memory bills
        await this.persistAllBillsToIndexedDb(this.bills);
      }
    } catch (err) {
      console.warn('Error loading from IndexedDB:', err);
    }
  }

  async persistBillToIndexedDb(bill) {
    if (!this.db || !bill || !bill.id) return;
    return new Promise((resolve, reject) => {
      try {
        const tx = this.db.transaction('bills', 'readwrite');
        const store = tx.objectStore('bills');
        store.put(bill);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      } catch (e) {
        reject(e);
      }
    });
  }

  async deleteBillFromIndexedDb(billId) {
    if (!this.db || !billId) return;
    return new Promise((resolve, reject) => {
      try {
        const tx = this.db.transaction('bills', 'readwrite');
        const store = tx.objectStore('bills');
        store.delete(billId);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      } catch (e) {
        reject(e);
      }
    });
  }

  async persistAllBillsToIndexedDb(billsList) {
    if (!this.db || !Array.isArray(billsList)) return;
    return new Promise((resolve, reject) => {
      try {
        const tx = this.db.transaction('bills', 'readwrite');
        const store = tx.objectStore('bills');
        billsList.forEach(b => {
          if (b && b.id) store.put(b);
        });
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
      } catch (e) {
        reject(e);
      }
    });
  }

  // Sync to localStorage as synchronous mirror
  syncToLocalStorage() {
    try {
      localStorage.setItem('shiv_railway_archive', JSON.stringify(this.bills));
      if (this.config) localStorage.setItem('shiv_railway_config', JSON.stringify(this.config));
      if (this.catalog && this.catalog.length > 0) localStorage.setItem('shiv_railway_catalog', JSON.stringify(this.catalog));
    } catch (e) {
      console.error('LocalStorage write error:', e);
    }
  }

  // ================= MODE SWITCHING & MANAGEMENT =================
  async setMode(newMode, notifyUser = true) {
    if (![this.MODE_CLOUD, this.MODE_LAN, this.MODE_LOCAL].includes(newMode)) {
      console.error('Invalid sync mode:', newMode);
      return false;
    }

    const previousMode = this.currentMode;
    this.currentMode = newMode;
    localStorage.setItem('shiv_sync_mode', newMode);

    this.log(`Switching Sync Mode: ${previousMode.toUpperCase()} -> ${newMode.toUpperCase()}`, 'info');

    // 1. Tear down previous driver listeners
    this.disconnectCurrentDriver(previousMode);

    // 2. Initialize new driver
    let success = false;
    if (newMode === this.MODE_CLOUD) {
      success = await this.initCloudMode();
    } else if (newMode === this.MODE_LAN) {
      success = await this.initLanMode();
    } else {
      // Local Mode
      this.isWsConnected = false;
      this.isCloudConnected = false;
      this.log('Mode C (Local Browser Only) active.', 'info');
      success = true;
    }

    // 3. Run two-way seamless data merge on switch
    if (success && newMode !== this.MODE_LOCAL) {
      this.log(`Initiating automatic two-way merge for ${newMode.toUpperCase()} mode...`, 'info');
      await this.forceSyncAll(false);
    }

    this.notifyStatusChange();

    if (notifyUser && typeof showToast === 'function') {
      const label = newMode === this.MODE_CLOUD ? '☁️ Cloud Sync (Firebase)' :
                    newMode === this.MODE_LAN ? '🖧 Local LAN Server' : '💾 Local Browser Only';
      showToast(`Sync Mode switched to: ${label}`, 'success');
    }

    return success;
  }

  disconnectCurrentDriver(mode) {
    if (mode === this.MODE_CLOUD) {
      if (typeof this.firestoreUnsubscribe === 'function') {
        this.firestoreUnsubscribe();
        this.firestoreUnsubscribe = null;
      }
      if (typeof this.firestoreDeletedUnsubscribe === 'function') {
        try { this.firestoreDeletedUnsubscribe(); } catch (e) {}
        this.firestoreDeletedUnsubscribe = null;
      }
      this.isCloudConnected = false;
    } else if (mode === this.MODE_LAN) {
      if (this.wsPingInterval) {
        clearInterval(this.wsPingInterval);
        this.wsPingInterval = null;
      }
      if (this.wsReconnectTimeout) {
        clearTimeout(this.wsReconnectTimeout);
        this.wsReconnectTimeout = null;
      }
      if (this.ws) {
        try {
          this.ws.close();
        } catch (e) {}
        this.ws = null;
      }
      this.isWsConnected = false;
      this.activeWorkstations = [];
    }
  }

  // ================= MODE A: CLOUD (FIREBASE FIRESTORE) =================
  async initCloudMode() {
    this.log('Connecting to Firebase Cloud Firestore...', 'info');

    if (!window.firebase) {
      this.log('Firebase SDK not loaded in browser. Check internet connection.', 'error');
      this.isCloudConnected = false;
      this.notifyStatusChange();
      return false;
    }

    if (!this.firebaseConfig || !this.firebaseConfig.projectId) {
      this.firebaseConfig = Object.assign({}, DEFAULT_FIREBASE_CONFIG);
    }

    try {
      // 1. Initialize or reuse Firebase App
      if (!firebase.apps || !firebase.apps.length) {
        this.firebaseApp = firebase.initializeApp(this.firebaseConfig);
      } else {
        this.firebaseApp = firebase.app();
      }

      // 2. Anonymous authentication if auth SDK is present
      if (firebase.auth) {
        try {
          if (!firebase.auth().currentUser) {
            await firebase.auth().signInAnonymously();
            this.log('Firebase anonymous session active.', 'info');
          }
        } catch (authErr) {
          console.warn('Firebase anonymous auth note:', authErr.message);
        }
      }

      this.firestore = firebase.firestore();

      // 3. Enable offline persistence for uninterrupted operation
      try {
        await this.firestore.enablePersistence({ synchronizeTabs: true });
        this.log('Firebase offline persistence enabled.', 'success');
      } catch (err) {
        if (err.code === 'failed-precondition') {
          console.warn('Multiple tabs open, persistence active in primary tab');
        } else if (err.code === 'unimplemented') {
          console.warn('Browser does not support persistence');
        }
      }

      // 4. Detach previous listeners if existing
      if (typeof this.firestoreUnsubscribe === 'function') {
        try { this.firestoreUnsubscribe(); } catch (e) {}
        this.firestoreUnsubscribe = null;
      }
      if (typeof this.firestoreDeletedUnsubscribe === 'function') {
        try { this.firestoreDeletedUnsubscribe(); } catch (e) {}
        this.firestoreDeletedUnsubscribe = null;
      }

      // 5. Attach Real-Time Listener on `nwr_bills` collection
      this.firestoreUnsubscribe = this.firestore.collection('nwr_bills')
        .onSnapshot({ includeMetadataChanges: true }, (snapshot) => {
          // Cloud connection is active
          this.isCloudConnected = true;
          this.notifyStatusChange();

          snapshot.docChanges().forEach((change) => {
            const billData = change.doc.data();
            const docId = change.doc.id;
            if (!billData && !docId) return;

            // Skip local writes echo
            if (billData && billData.workstationId === this.workstationId && snapshot.metadata.hasPendingWrites) {
              return;
            }

            if (change.type === 'added' || change.type === 'modified') {
              if (billData?.deleted || this.isBillDeleted(billData?.id || docId, billData?.billNo)) {
                this.handleRemoteBillDelete(billData?.id || docId, billData?.workstationId || 'Cloud');
                // Ensure document is permanently purged from nwr_bills
                this.firestore.collection('nwr_bills').doc(docId).delete().catch(() => {});
              } else {
                this.handleRemoteBillUpdate(billData, billData.workstationId || 'Cloud');
              }
            } else if (change.type === 'removed') {
              this.handleRemoteBillDelete(docId, billData?.workstationId || 'Cloud');
            }
          });
        }, (error) => {
          console.warn('Firestore snapshot listener event:', error.message);
          this.isCloudConnected = (typeof navigator !== 'undefined' && navigator.onLine !== false);
          this.notifyStatusChange();
        });

      // 6. Attach Real-Time Listener on `nwr_deleted_bills` collection
      this.firestoreDeletedUnsubscribe = this.firestore.collection('nwr_deleted_bills')
        .onSnapshot((snapshot) => {
          snapshot.docChanges().forEach((change) => {
            if (change.type === 'added' || change.type === 'modified') {
              const delId = change.doc.id;
              if (delId) {
                this.handleRemoteBillDelete(delId, 'Cloud');
              }
            }
          });
        }, (err) => {
          console.warn('Firestore deleted-bills listener note:', err.message);
        });

      this.isCloudConnected = true;
      this.log('Connected to Firebase Firestore. Real-time sync active.', 'success');
      this.drainSyncQueue();
      return true;
    } catch (err) {
      console.warn('Firebase initialization notice:', err.message);
      this.log(`Firebase initialization note: ${err.message}`, 'warning');
      this.isCloudConnected = true;
      this.notifyStatusChange();
      return true;
    }
  }

  async pushBillToCloud(bill) {
    if (!bill || !bill.id) return false;

    // Never push a permanently deleted bill to cloud
    if (this.isBillDeleted(bill.id, bill.billNo)) {
      console.warn(`[SYNC] Dropped push of permanently deleted bill #${bill.billNo} (${bill.id}) to Cloud.`);
      return false;
    }

    if (!this.firestore) {
      await this.initCloudMode();
    }
    if (!this.firestore) {
      this.enqueueOfflineAction({ type: 'SAVE_BILL', bill });
      return false;
    }
    try {
      await this.firestore.collection('nwr_bills').doc(bill.id).set(bill, { merge: true });
      this.isCloudConnected = true;
      this.notifyStatusChange();
      return true;
    } catch (err) {
      console.warn('Error writing bill to Firestore (queued for sync):', err.message);
      this.enqueueOfflineAction({ type: 'SAVE_BILL', bill });
      return false;
    }
  }

  async deleteBillFromCloud(billId) {
    if (!billId) return false;

    if (!this.firestore) {
      await this.initCloudMode();
    }
    if (!this.firestore) {
      this.enqueueOfflineAction({ type: 'DELETE_BILL', billId });
      return false;
    }
    try {
      // 1. Permanently hard-delete the document from nwr_bills
      await this.firestore.collection('nwr_bills').doc(billId).delete();

      // 2. Record permanent tombstone in nwr_deleted_bills so peer workstations purge and cannot restore it
      await this.firestore.collection('nwr_deleted_bills').doc(billId).set({
        id: billId,
        deletedAt: new Date().toISOString(),
        workstationId: this.workstationId
      }, { merge: true });

      this.isCloudConnected = true;
      this.notifyStatusChange();
      return true;
    } catch (err) {
      console.warn('Error deleting bill in Firestore (queued):', err.message);
      this.enqueueOfflineAction({ type: 'DELETE_BILL', billId });
      return false;
    }
  }

  // ================= MODE B: LOCAL LAN SERVER (WEBSOCKET + REST) =================
  async initLanMode() {
    this.log(`Connecting to Local LAN Server at ${this.lanServerUrl}...`, 'info');

    // 1. First test REST connection
    try {
      const res = await fetch(`${this.lanServerUrl}/api/health`, { method: 'GET', signal: AbortSignal.timeout(3500) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const health = await res.json();
      this.log(`LAN Server verified online (${health.totalBills} server records).`, 'success');
      this.activeWorkstations = health.activeWorkstations || [];
    } catch (err) {
      this.log(`Could not reach LAN Server at ${this.lanServerUrl}. Check if server is running.`, 'warning');
      this.isWsConnected = false;
      this.notifyStatusChange();
      this.scheduleLanReconnect();
      return false;
    }

    // 2. Connect WebSocket
    return this.connectLanWebSocket();
  }

  connectLanWebSocket() {
    return new Promise((resolve) => {
      try {
        let wsUrl = this.lanServerUrl.replace(/^http/, 'ws');
        if (!wsUrl.endsWith('/ws')) wsUrl += '/ws';

        this.log(`Opening WebSocket connection to ${wsUrl}...`, 'info');
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          this.isWsConnected = true;
          this.log(`WebSocket connected to LAN Server!`, 'success');

          // Send Handshake
          this.ws.send(JSON.stringify({
            type: 'HANDSHAKE',
            workstationId: this.workstationId,
            workstationName: this.workstationName
          }));

          // Request fresh master dataset from server
          this.ws.send(JSON.stringify({
            type: 'REQUEST_FULL_SYNC'
          }));

          // Start Heartbeat
          if (this.wsPingInterval) clearInterval(this.wsPingInterval);
          this.wsPingInterval = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
              this.ws.send(JSON.stringify({ type: 'PING' }));
            }
          }, 15000);

          // Drain any queued offline writes
          this.drainSyncQueue();
          this.notifyStatusChange();
          resolve(true);
        };

        this.ws.onmessage = (event) => {
          try {
            const msg = JSON.parse(event.data);
            this.handleLanWebSocketMessage(msg);
          } catch (e) {
            console.error('Error parsing LAN WS message:', e);
          }
        };

        this.ws.onclose = () => {
          this.isWsConnected = false;
          this.log('LAN WebSocket connection closed.', 'warning');
          this.notifyStatusChange();
          this.scheduleLanReconnect();
        };

        this.ws.onerror = (err) => {
          this.isWsConnected = false;
          console.warn('LAN WebSocket error:', err);
          this.notifyStatusChange();
          resolve(false);
        };
      } catch (err) {
        console.error('WebSocket creation error:', err);
        this.isWsConnected = false;
        this.notifyStatusChange();
        resolve(false);
      }
    });
  }

  scheduleLanReconnect() {
    if (this.currentMode !== this.MODE_LAN) return;
    if (this.wsReconnectTimeout) clearTimeout(this.wsReconnectTimeout);

    this.wsReconnectTimeout = setTimeout(() => {
      if (this.currentMode === this.MODE_LAN && !this.isWsConnected) {
        this.log('Attempting LAN Server auto-reconnection...', 'info');
        this.initLanMode();
      }
    }, 4000);
  }

  handleLanWebSocketMessage(msg) {
    switch (msg.type) {
      case 'CONNECTED':
      case 'HANDSHAKE_ACK': {
        this.activeWorkstations = msg.activeWorkstations || [];
        this.notifyPeersChange();
        break;
      }

      case 'FULL_SYNC_DATA': {
        if (Array.isArray(msg.deletedBillIds)) {
          msg.deletedBillIds.forEach(id => this.markBillPermanentlyDeleted(id));
        }
        if (Array.isArray(msg.bills)) {
          const activeBills = msg.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
          this.reconcileLocalArrays(activeBills);
          this.syncToLocalStorage();
          this.persistAllBillsToIndexedDb(this.bills);
          this.log(`Received full synchronized archive from LAN Server (${this.bills.length} bills).`, 'success');
          this.notifyRemoteBillUpdate(null, false);
        }
        if (msg.config) {
          this.config = Object.assign({}, this.config, msg.config);
          this.syncToLocalStorage();
          if (typeof syncConfigToUi === 'function') syncConfigToUi();
        }
        break;
      }

      case 'ALL_BILLS_CLEARED': {
        this.bills = [];
        this.syncToLocalStorage();
        if (this.db) {
          const tx = this.db.transaction('bills', 'readwrite');
          tx.objectStore('bills').clear();
        }
        this.log(`All bills cleared remotely on LAN by ${msg.workstationId}`, 'warning');
        this.notifyRemoteBillDelete('*');
        break;
      }

      case 'PEER_JOINED': {
        this.activeWorkstations = msg.activeWorkstations || [];
        this.log(`New computer connected on LAN: ${msg.workstationId} (${msg.workstationName || ''})`, 'info');
        if (typeof showToast === 'function') {
          showToast(`💻 Workstation joined: ${msg.workstationName || msg.workstationId}`, 'info');
        }
        this.notifyPeersChange();
        break;
      }

      case 'PEER_LEFT': {
        this.activeWorkstations = msg.activeWorkstations || [];
        this.log(`Workstation disconnected: ${msg.workstationId}`, 'info');
        this.notifyPeersChange();
        break;
      }

      case 'BILL_UPDATED': {
        if (msg.bill && msg.workstationId !== this.workstationId) {
          if (!this.isBillDeleted(msg.bill.id, msg.bill.billNo)) {
            this.handleRemoteBillUpdate(msg.bill, msg.workstationName || msg.workstationId);
          }
        }
        break;
      }

      case 'BILL_SOFT_DELETED': {
        if (msg.billId && msg.workstationId !== this.workstationId) {
          this.handleRemoteBillSoftDelete(msg.billId, msg.isDeleted, msg.bill, msg.workstationName || msg.workstationId);
        }
        break;
      }

      case 'BILL_DELETED': {
        if (msg.billId) {
          this.markBillPermanentlyDeleted(msg.billId, msg.billNo);
          if (msg.workstationId !== this.workstationId) {
            this.handleRemoteBillDelete(msg.billId, msg.workstationName || msg.workstationId);
          }
        }
        break;
      }

      case 'CONFIG_UPDATED': {
        if (msg.config && msg.workstationId !== this.workstationId) {
          this.config = msg.config;
          this.syncToLocalStorage();
          if (typeof syncConfigToUi === 'function') syncConfigToUi();
          this.log(`Facility settings updated remotely by ${msg.workstationId}`, 'info');
        }
        break;
      }

      case 'BATCH_SYNC_OCCURRED': {
        if (msg.workstationId !== this.workstationId) {
          this.log(`Remote batch sync detected from ${msg.workstationId}. Refreshing records...`, 'info');
          this.forceSyncAll(false);
        }
        break;
      }

      case 'PONG': {
        // Heartbeat ACK
        break;
      }
    }
  }

  async pushBillToLan(bill) {
    if (!bill || !bill.id) return false;

    // Never push a permanently deleted bill to LAN
    if (this.isBillDeleted(bill.id, bill.billNo)) {
      console.warn(`[SYNC] Dropped push of permanently deleted bill #${bill.billNo} (${bill.id}) to LAN.`);
      return false;
    }

    bill.updatedAt = bill.updatedAt || new Date().toISOString();
    bill.workstationId = this.workstationId;

    // Send via WebSocket for instant broadcast
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'BILL_UPDATED',
        bill: bill,
        workstationId: this.workstationId,
        workstationName: this.workstationName
      }));
    }

    // Also persist via REST API
    try {
      await fetch(`${this.lanServerUrl}/api/bills`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ bill, workstationId: this.workstationId })
      });
      return true;
    } catch (err) {
      console.warn('LAN Server REST write failed, enqueued offline:', err);
      this.enqueueOfflineAction({ type: 'SAVE_BILL', bill });
      return false;
    }
  }

  async pushSoftDeleteToLan(bill) {
    if (!bill || !bill.id) return false;

    // Send soft-delete action via WebSocket
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'BILL_SOFT_DELETED',
        billId: bill.id,
        billNo: bill.billNo,
        isDeleted: bill.isDeleted,
        status: bill.status,
        deletedAt: bill.deletedAt,
        restoredAt: bill.restoredAt,
        bill: bill,
        workstationId: this.workstationId,
        workstationName: this.workstationName
      }));
    }

    // Call REST endpoint
    try {
      await fetch(`${this.lanServerUrl}/api/bills/${bill.id}/soft-delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          isDeleted: bill.isDeleted,
          status: bill.status,
          deletedAt: bill.deletedAt,
          workstationId: this.workstationId
        })
      });
      return true;
    } catch (err) {
      console.warn('LAN Server REST soft-delete write failed, enqueued offline:', err);
      this.enqueueOfflineAction({ type: 'SOFT_DELETE_BILL', billId: bill.id, bill });
      return false;
    }
  }

  async deleteBillFromLan(billId) {
    // Send via WebSocket
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        type: 'BILL_DELETED',
        billId: billId,
        workstationId: this.workstationId,
        workstationName: this.workstationName
      }));
    }

    // Call REST
    try {
      await fetch(`${this.lanServerUrl}/api/bills/${billId}?workstationId=${this.workstationId}`, {
        method: 'DELETE'
      });
      return true;
    } catch (err) {
      this.enqueueOfflineAction({ type: 'DELETE_BILL', billId });
      return false;
    }
  }

  // ================= CONFLICT PREVENTION & ATOMIC BILL NUMBERS =================
  resolveMonthlyPrefix(rawPattern, yearMonth) {
    const parts = (yearMonth || new Date().toISOString().slice(0, 7)).split('-');
    const yyyy = parseInt(parts[0], 10) || new Date().getFullYear();
    const mm = parts[1] || String(new Date().getMonth() + 1).padStart(2, '0');
    const yy = String(yyyy).slice(-2);
    const mInt = parseInt(mm, 10);
    const fyStart = mInt >= 4 ? yyyy : yyyy - 1;
    const fyEnd = String(fyStart + 1).slice(-2);
    const fyStr = `${fyStart}-${fyEnd}`;
    const monthNames = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    const mmm = monthNames[mInt - 1] || 'GEN';

    let pattern = rawPattern || "NWR/2026-27/{MM}/";
    if (pattern.includes('{MM}') || pattern.includes('{YYYY}') || pattern.includes('{FY}') || pattern.includes('{MMM}')) {
      return pattern
        .replace(/{YYYY}/g, yyyy)
        .replace(/{YY}/g, yy)
        .replace(/{MM}/g, mm)
        .replace(/{MMM}/g, mmm)
        .replace(/{FY}/g, fyStr);
    }

    if (/\/\d{2}\/$/.test(pattern)) {
      return pattern.replace(/\/\d{2}\/$/, `/${mm}/`);
    }

    if (pattern.endsWith('/')) {
      return `${pattern}${mm}/`;
    }
    return `${pattern}/${mm}/`;
  }

  async getNextSequentialBillNumber(rawPrefix = "NWR/2026-27/{MM}/", dateStr = "") {
    const targetDate = dateStr || new Date().toISOString().slice(0, 10);
    const targetMonth = targetDate.slice(0, 7); // e.g. "2026-10"
    const prefix = this.resolveMonthlyPrefix(rawPrefix, targetMonth);

    // In LAN Mode, ask server for atomic counter scoped to this month
    if (this.currentMode === this.MODE_LAN && this.isWsConnected) {
      try {
        const res = await fetch(`${this.lanServerUrl}/api/next-bill-number?prefix=${encodeURIComponent(prefix)}&month=${encodeURIComponent(targetMonth)}&date=${encodeURIComponent(targetDate)}`, {
          signal: AbortSignal.timeout(2000)
        });
        if (res.ok) {
          const data = await res.json();
          if (data && data.billNo) {
            const out = {
              billNo: data.billNo,
              recordId: data.recordId || `bill_${targetMonth.replace('-', '_')}_${String(data.sequence).padStart(5, '0')}`,
              sequence: data.sequence,
              month: targetMonth,
              prefix: prefix
            };
            out.toString = function() { return this.billNo; };
            return out;
          }
        }
      } catch (e) {
        console.warn('Failed to get atomic number from LAN server, using local calculation:', e);
      }
    }

    // Fallback or Cloud/Local mode: compute highest from all known bills in this month
    let maxNum = 0;
    this.bills.forEach(item => {
      if (!item) return;
      const itemMonth = item.date ? String(item.date).slice(0, 7) : '';
      const matchesPrefix = item.billNo && item.billNo.startsWith(prefix);
      if (itemMonth === targetMonth || matchesPrefix) {
        const match = String(item.billNo || '').match(/(\d+)\s*$/);
        if (match) {
          const parsed = parseInt(match[1], 10);
          if (!isNaN(parsed) && parsed > maxNum) {
            maxNum = parsed;
          }
        }
      }
    });

    const nextSeq = maxNum + 1;
    const formattedSeq = String(nextSeq).padStart(5, '0');
    const billNo = `${prefix}${formattedSeq}`;
    const recordId = `bill_${targetMonth.replace('-', '_')}_${formattedSeq}`;

    const result = {
      billNo: billNo,
      recordId: recordId,
      sequence: nextSeq,
      month: targetMonth,
      prefix: prefix
    };
    result.toString = function() { return this.billNo; };
    return result;
  }

  // Prevent simultaneous clash right before saving
  resolveBillNumberClash(record) {
    if (!record || !record.billNo) return record;

    // Check if another bill already exists with identical billNo but different id
    const clash = this.bills.find(b => b.billNo === record.billNo && b.id !== record.id);
    if (clash) {
      // Find highest number and increment
      const prefix = record.billNo.replace(/\d+$/, '');
      let maxNum = 0;
      this.bills.forEach(item => {
        if (item.billNo && item.billNo.startsWith(prefix)) {
          const numPart = item.billNo.replace(prefix, '').trim();
          const parsed = parseInt(numPart, 10);
          if (!isNaN(parsed) && parsed > maxNum) maxNum = parsed;
        }
      });
      const newBillNo = `${prefix}${String(maxNum + 1).padStart(5, '0')}`;
      this.log(`⚠️ Bill Number Collision Avoided: ${record.billNo} -> ${newBillNo}`, 'warning');
      if (typeof showToast === 'function') {
        showToast(`⚠️ Bill #${record.billNo} was already used by another PC. Auto-assigned: #${newBillNo}`, 'warning');
      }
      record.billNo = newBillNo;
    }
    return record;
  }

  // ================= PATIENT DUPLICATION PREVENTION =================
  normalizeUmid(umid) {
    if (!umid) return '';
    return String(umid).toUpperCase().replace(/[^A-Z0-9]/g, '');
  }

  normalizeBillNo(billNo) {
    if (!billNo) return '';
    return String(billNo).trim().toUpperCase().replace(/\s+/g, '');
  }

  normalizeName(name) {
    if (!name) return '';
    return String(name).trim().toLowerCase().replace(/\s+/g, ' ');
  }

  findDuplicatePatient(record) {
    if (!record) return null;

    const targetId = record.id || '';
    const recBillNo = this.normalizeBillNo(record.billNo);
    const recDate = String(record.date || '').trim();
    const recUmid = this.normalizeUmid(record.umidId);
    const recName = this.normalizeName(record.patientName);

    if (!recBillNo && !recUmid && !recName) return null;

    return this.bills.find(b => {
      // Soft-deleted (inactive) records do not cause duplicate blocks
      if (b.isDeleted || b.status === 'inactive') return false;

      // Allow updating the exact same bill being edited
      if (targetId && b.id === targetId) return false;

      // 1. Unique Identifier: Bill Number match (globally unique across system)
      const bBillNo = this.normalizeBillNo(b.billNo);
      if (recBillNo && bBillNo && recBillNo === bBillNo) {
        return true;
      }

      // 2. Unique Identifier: UMID ID match on the same date
      if (recDate && String(b.date || '').trim() === recDate) {
        const bUmid = this.normalizeUmid(b.umidId);
        if (recUmid && bUmid && recUmid === bUmid) {
          return true;
        }

        // 3. Fallback: Patient name match on same date if UMID is blank
        if (!recUmid && !bUmid) {
          const bName = this.normalizeName(b.patientName);
          if (recName && bName && recName === bName) {
            return true;
          }
        }
      }

      return false;
    }) || null;
  }

  getRecordOriginalOrder(record) {
    if (!record) return Number.MAX_SAFE_INTEGER;
    if (record.createdAt) {
      const t = new Date(record.createdAt).getTime();
      if (!isNaN(t) && t > 0) return t;
    }
    if (record.id) {
      const m = String(record.id).match(/bill_(\d{10,13})/);
      if (m) {
        const t = parseInt(m[1], 10);
        if (!isNaN(t) && t > 0) return t;
      }
    }
    if (record.date) {
      const timePart = (record.time && /^\d{1,2}:\d{2}/.test(record.time)) ? record.time : '00:00';
      const t = new Date(`${record.date}T${timePart}`).getTime();
      if (!isNaN(t) && t > 0) return t;
    }
    if (record.billNo) {
      const match = String(record.billNo).match(/(\d+)\s*$/);
      if (match) return parseInt(match[1], 10);
    }
    return Number.MAX_SAFE_INTEGER;
  }

  getDuplicateClusters(records = this.bills) {
    const activeBills = (records || []).filter(b => b && !b.isDeleted && b.status !== 'inactive');

    const parent = new Map();
    const find = (id) => {
      if (!parent.has(id)) parent.set(id, id);
      if (parent.get(id) === id) return id;
      const root = find(parent.get(id));
      parent.set(id, root);
      return root;
    };
    const union = (idA, idB) => {
      const rootA = find(idA);
      const rootB = find(idB);
      if (rootA !== rootB) parent.set(rootA, rootB);
    };

    const billNoMap = new Map();
    const umidDateMap = new Map();
    const nameDateMap = new Map();

    for (const bill of activeBills) {
      find(bill.id);

      const normBill = this.normalizeBillNo(bill.billNo);
      if (normBill) {
        if (billNoMap.has(normBill)) union(bill.id, billNoMap.get(normBill));
        else billNoMap.set(normBill, bill.id);
      }

      const dateStr = String(bill.date || '').trim();
      const normUmid = this.normalizeUmid(bill.umidId);
      const normName = this.normalizeName(bill.patientName);

      if (normUmid && dateStr) {
        const key = normUmid + '___' + dateStr;
        if (umidDateMap.has(key)) union(bill.id, umidDateMap.get(key));
        else umidDateMap.set(key, bill.id);
      } else if (normName && dateStr) {
        const key = normName + '___' + dateStr;
        if (nameDateMap.has(key)) union(bill.id, nameDateMap.get(key));
        else nameDateMap.set(key, bill.id);
      }
    }

    const clusterMap = new Map();
    for (const bill of activeBills) {
      const root = find(bill.id);
      if (!clusterMap.has(root)) clusterMap.set(root, []);
      clusterMap.get(root).push(bill);
    }

    const duplicateGroups = [];
    clusterMap.forEach((bills) => {
      if (bills.length > 1) {
        // Sort to keep the FIRST ORIGINAL INITIAL ENTRY at index 0
        bills.sort((a, b) => {
          const orderA = this.getRecordOriginalOrder(a);
          const orderB = this.getRecordOriginalOrder(b);
          if (orderA !== orderB) return orderA - orderB;

          const numA = (String(a.billNo || '').match(/(\d+)\s*$/) || [])[1] || 0;
          const numB = (String(b.billNo || '').match(/(\d+)\s*$/) || [])[1] || 0;
          const diffSeq = parseInt(numA, 10) - parseInt(numB, 10);
          if (!isNaN(diffSeq) && diffSeq !== 0) return diffSeq;

          return String(a.id || '').localeCompare(String(b.id || ''));
        });

        const primary = bills[0]; // Untouched original entry
        const subsequentDuplicates = bills.slice(1); // Redundant copies to be removed

        duplicateGroups.push({
          primary,
          duplicates: subsequentDuplicates,
          allBills: bills,
          count: subsequentDuplicates.length
        });
      }
    });

    return duplicateGroups;
  }

  // ================= CRUD OPERATIONS (UNIFIED DATA LAYER) =================
  async saveBill(record, options = {}) {
    if (!record.id) {
      const targetMonth = (record.date ? String(record.date).slice(0, 7) : new Date().toISOString().slice(0, 7));
      let maxMonthSeq = 0;
      this.bills.forEach(item => {
        if (!item) return;
        const itemMonth = item.date ? String(item.date).slice(0, 7) : '';
        if (itemMonth === targetMonth) {
          const m = String(item.id || '').match(/bill_\d{4}_\d{2}_(\d+)/);
          if (m) {
            const p = parseInt(m[1], 10);
            if (!isNaN(p) && p > maxMonthSeq) maxMonthSeq = p;
          }
        }
      });
      const nextMonthSeq = maxMonthSeq + 1;
      record.id = `bill_${targetMonth.replace('-', '_')}_${String(nextMonthSeq).padStart(5, '0')}`;
    }

    // Check if ID was permanently deleted
    if (this.isBillDeleted(record.id, record.billNo)) {
      const err = new Error(`Cannot save: Bill #${record.billNo} (${record.id}) has been permanently deleted from records.`);
      this.log(`⚠️ Blocked attempt to recreate permanently deleted bill #${record.billNo}`, 'warning');
      throw err;
    }

    // Duplicate patient entry prevention on the same day
    if (!options.allowDuplicate && !options.skipDuplicateCheck) {
      const dup = this.findDuplicatePatient(record);
      if (dup) {
        const dupMsg = `Duplicate patient entry blocked: Patient "${dup.patientName}" (UMID: ${dup.umidId || 'N/A'}) already registered on ${dup.date} with Bill #${dup.billNo}.`;
        this.log(`⚠️ ${dupMsg}`, 'warning');
        throw new Error(dupMsg);
      }
    }

    if (!record.createdAt) {
      record.createdAt = new Date().toISOString();
    }
    record.updatedAt = new Date().toISOString();
    record.workstationId = this.workstationId;

    // Resolve any multi-computer voucher number clashes
    this.resolveBillNumberClash(record);

    // 1. Update in-memory array
    const existingIdx = this.bills.findIndex(b => b.id === record.id || b.billNo === record.billNo);
    if (existingIdx >= 0) {
      this.bills[existingIdx] = record;
    } else {
      this.bills.unshift(record);
    }

    // 2. Persist locally (IndexedDB + localStorage)
    this.syncToLocalStorage();
    await this.persistBillToIndexedDb(record);

    // 3. Propagate to active remote driver
    if (this.currentMode === this.MODE_CLOUD) {
      await this.pushBillToCloud(record);
    } else if (this.currentMode === this.MODE_LAN) {
      await this.pushBillToLan(record);
    }

    this.log(`Saved Bill #${record.billNo} (${record.patientName}) locally and synced.`, 'success');
    return record;
  }

  // ================= TWO-TIER DELETION SYSTEM =================

  /**
   * Tier 1: Soft Delete (Inactivate or Restore)
   * Flags record as inactive (isDeleted: true, status: 'inactive') without removing from database.
   * Excluded from active claim sheets, bill counts, and duplicate checking.
   * Can be restored to active state at any time.
   */
  async softDeleteBill(billId, isDeleted = true, reason = '') {
    if (!billId) return false;
    const bill = this.bills.find(b => b.id === billId || b.billNo === billId);
    if (!bill) return false;

    bill.isDeleted = isDeleted !== false;
    bill.status = bill.isDeleted ? 'inactive' : 'active';
    bill.deletedAt = bill.isDeleted ? new Date().toISOString() : null;
    if (!bill.isDeleted) {
      bill.restoredAt = new Date().toISOString();
      bill.restoredBy = this.workstationId;
    } else {
      bill.deletedBy = this.workstationId;
      if (reason) bill.deletionReason = reason;
    }
    bill.updatedAt = new Date().toISOString();

    // 1. Persist locally (IndexedDB + localStorage)
    this.syncToLocalStorage();
    await this.persistBillToIndexedDb(bill);

    // 2. Propagate to active remote driver
    if (this.currentMode === this.MODE_CLOUD) {
      await this.pushBillToCloud(bill);
    } else if (this.currentMode === this.MODE_LAN) {
      await this.pushSoftDeleteToLan(bill);
    }

    const stateDesc = bill.isDeleted ? 'flagged as inactive (soft-deleted)' : 'restored to active';
    this.log(`Bill #${bill.billNo} ${stateDesc}.`, 'info');
    return true;
  }

  async restoreBill(billId) {
    return this.softDeleteBill(billId, false);
  }

  /**
   * Tier 2: Hard Delete (Permanent Purge)
   * Irreversibly deletes record from database, in-memory array, IndexedDB, and localStorage.
   * Added to permanent tombstone blacklist so peers cannot restore it.
   */
  async hardDeleteBill(billId) {
    return this.deleteBill(billId);
  }

  async deleteBill(billId) {
    if (!billId) return false;
    const bill = this.bills.find(b => b.id === billId || b.billNo === billId);
    const actualId = bill ? bill.id : billId;
    const billNo = bill ? bill.billNo : billId;

    // 1. Mark permanently deleted in blacklist (cannot be restored)
    this.markBillPermanentlyDeleted(actualId, billNo);

    // 2. Remove from in-memory array
    this.bills = this.bills.filter(b => b.id !== actualId && b.billNo !== billNo);

    // 3. Purge from offline queue
    this.syncQueue = this.syncQueue.filter(item => {
      if (item.bill && (item.bill.id === actualId || item.bill.billNo === billNo)) return false;
      if (item.billId === actualId || item.billId === billNo) return false;
      return true;
    });

    // 4. Remove locally (IndexedDB + localStorage)
    this.syncToLocalStorage();
    await this.deleteBillFromIndexedDb(actualId);

    // 5. Propagate permanent deletion to active remote driver
    if (this.currentMode === this.MODE_CLOUD) {
      await this.deleteBillFromCloud(actualId);
    } else if (this.currentMode === this.MODE_LAN) {
      await this.deleteBillFromLan(actualId);
    }

    this.log(`Permanently deleted Bill #${billNo} (${actualId}) from database. Cannot be restored.`, 'info');
    return true;
  }

  async clearAllBills() {
    const count = this.bills.length;
    const oldBills = [...this.bills];

    // 1. Mark all records permanently deleted in blacklist
    oldBills.forEach(b => {
      if (b) this.markBillPermanentlyDeleted(b.id, b.billNo);
    });

    // 2. Clear local memory and queue
    this.bills = [];
    this.syncQueue = [];
    this.syncToLocalStorage();

    // 3. Clear IndexedDB
    if (this.db) {
      try {
        const tx = this.db.transaction('bills', 'readwrite');
        tx.objectStore('bills').clear();
      } catch (e) {
        console.warn('Error clearing IndexedDB:', e);
      }
    }

    // 4. Propagate clear to Cloud
    if (this.currentMode === this.MODE_CLOUD && this.firestore) {
      try {
        for (const b of oldBills) {
          if (b && b.id) {
            await this.firestore.collection('nwr_bills').doc(b.id).delete().catch(() => {});
            await this.firestore.collection('nwr_deleted_bills').doc(b.id).set({
              id: b.id,
              deletedAt: new Date().toISOString(),
              workstationId: this.workstationId
            }).catch(() => {});
          }
        }
      } catch (e) {
        console.warn('Error clearing cloud collection:', e);
      }
    }

    // 5. Propagate clear to LAN
    if (this.currentMode === this.MODE_LAN) {
      try {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify({
            type: 'CLEAR_ALL_BILLS',
            workstationId: this.workstationId
          }));
        }
        await fetch(`${this.lanServerUrl}/api/bills?workstationId=${encodeURIComponent(this.workstationId)}`, {
          method: 'DELETE'
        }).catch(() => {});
      } catch (e) {
        console.warn('Error clearing LAN server bills:', e);
      }
    }

    this.log(`Permanently cleared all ${count} bills from database. Cannot be restored.`, 'warning');
    return true;
  }

  getBills() {
    return this.bills;
  }

  setBills(newBills) {
    this.bills = Array.isArray(newBills) ? [...newBills] : [];
    this.syncToLocalStorage();
    this.persistAllBillsToIndexedDb(this.bills);
    if (this.currentMode === this.MODE_LAN) {
      this.forceSyncAll(false);
    }
  }

  async pushConfig(config) {
    this.config = config;
    this.syncToLocalStorage();
    if (this.currentMode === this.MODE_LAN) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({
          type: 'CONFIG_UPDATED',
          config: config,
          workstationId: this.workstationId
        }));
      }
      try {
        await fetch(`${this.lanServerUrl}/api/config`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ config, workstationId: this.workstationId })
        });
      } catch (e) {
        console.warn('Failed to push config via REST:', e);
      }
    }
  }

  // ================= REMOTE UPDATES HANDLING & LIVE NOTIFICATIONS =================
  handleRemoteBillUpdate(remoteBill, sourceName) {
    if (!remoteBill || !remoteBill.id) return;

    const existingIdx = this.bills.findIndex(b => b.id === remoteBill.id);
    let isNew = false;

    if (existingIdx >= 0) {
      const localBill = this.bills[existingIdx];
      const localTime = new Date(localBill.updatedAt || localBill.createdAt || 0).getTime();
      const remoteTime = new Date(remoteBill.updatedAt || remoteBill.createdAt || 0).getTime();

      // Only update if remote is newer or equal
      if (remoteTime >= localTime) {
        this.bills[existingIdx] = remoteBill;
      } else {
        // Local is newer, don't overwrite
        return;
      }
    } else {
      isNew = true;
      this.bills.unshift(remoteBill);
    }

    // Update local persistent mirrors
    this.syncToLocalStorage();
    this.persistBillToIndexedDb(remoteBill);

    // Trigger subtle live toast notification
    if (typeof showToast === 'function') {
      const icon = this.currentMode === this.MODE_CLOUD ? '☁️' : '🖧';
      const actionText = isNew ? 'registered patient' : 'updated bill';
      showToast(`${icon} ${sourceName} ${actionText}: ${remoteBill.patientName} (#${remoteBill.billNo})`, 'info');
    }

    this.log(`Remote update received from ${sourceName}: #${remoteBill.billNo} (${remoteBill.patientName})`, 'info');

    // Notify UI to re-render archive table and update badges
    this.notifyRemoteBillUpdate(remoteBill, isNew);
  }

  handleRemoteBillSoftDelete(billId, isDeleted, remoteBill, sourceName) {
    if (!billId) return;

    let target = this.bills.find(b => b.id === billId || b.billNo === billId);
    if (!target && remoteBill) {
      target = remoteBill;
      this.bills.unshift(target);
    }
    if (!target) return;

    target.isDeleted = isDeleted !== false;
    target.status = target.isDeleted ? 'inactive' : 'active';
    target.deletedAt = target.isDeleted ? (remoteBill?.deletedAt || new Date().toISOString()) : null;
    if (!target.isDeleted) {
      target.restoredAt = remoteBill?.restoredAt || new Date().toISOString();
    }
    target.updatedAt = remoteBill?.updatedAt || new Date().toISOString();

    this.syncToLocalStorage();
    this.persistBillToIndexedDb(target);

    if (typeof showToast === 'function') {
      const icon = this.currentMode === this.MODE_CLOUD ? '☁️' : '🖧';
      const stateText = target.isDeleted ? 'moved to Inactive' : 'restored to Active';
      showToast(`${icon} ${sourceName} ${stateText}: Bill #${target.billNo}`, 'info');
    }

    this.log(`Remote soft-delete event from ${sourceName}: #${target.billNo} isDeleted=${target.isDeleted}`, 'info');
    this.notifyRemoteBillSoftDelete(target);
    this.notifyRemoteBillUpdate(target, false);
  }

  handleRemoteBillDelete(billId, sourceName) {
    if (!billId) return;

    // Mark permanently deleted in blacklist so it can never be restored
    this.markBillPermanentlyDeleted(billId);

    const existing = this.bills.find(b => b.id === billId || b.billNo === billId);
    const billNo = existing ? existing.billNo : billId;

    this.bills = this.bills.filter(b => b.id !== billId && b.billNo !== billId);

    this.syncToLocalStorage();
    this.deleteBillFromIndexedDb(billId);

    if (typeof showToast === 'function') {
      const icon = this.currentMode === this.MODE_CLOUD ? '☁️' : '🖧';
      showToast(`${icon} ${sourceName} permanently deleted Bill #${billNo}`, 'info');
    }

    this.log(`Remote permanent delete received from ${sourceName}: Bill #${billNo}`, 'info');
    this.notifyRemoteBillDelete(billId);
  }

  // ================= SEAMLESS TWO-WAY DATA MERGING =================
  async forceSyncAll(notifyUser = true) {
    this.log(`Starting two-way reconciliation in ${this.currentMode.toUpperCase()} mode...`, 'info');

    if (this.currentMode === this.MODE_LAN) {
      try {
        const payload = {
          bills: this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo)),
          workstationId: this.workstationId
        };
        const res = await fetch(`${this.lanServerUrl}/api/sync`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        
        if (data) {
          // 1. Process server deletedBillIds
          if (Array.isArray(data.deletedBillIds)) {
            data.deletedBillIds.forEach(id => this.markBillPermanentlyDeleted(id));
          }

          // 2. Filter active bills
          if (Array.isArray(data.bills)) {
            const activeServerBills = data.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
            this.reconcileLocalArrays(activeServerBills);
            this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
            this.syncToLocalStorage();
            await this.persistAllBillsToIndexedDb(this.bills);
          }
          
          this.log(`LAN Sync completed: ${this.bills.length} total active records reconciled.`, 'success');
          if (notifyUser && typeof showToast === 'function') {
            showToast(`🔄 Two-way LAN Sync complete. ${this.bills.length} bills synchronized.`, 'success');
          }
          this.notifyRemoteBillUpdate(null, false);
          return true;
        }
      } catch (err) {
        this.log(`LAN Force Sync failed: ${err.message}`, 'error');
        if (notifyUser && typeof showToast === 'function') {
          showToast(`Sync Error: Could not reach LAN server (${err.message})`, 'error');
        }
        return false;
      }
    } else if (this.currentMode === this.MODE_CLOUD) {
      if (!this.firestore) {
        await this.initCloudMode();
      }

      if (!this.firestore) {
        this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
        this.syncToLocalStorage();
        await this.persistAllBillsToIndexedDb(this.bills);
        if (notifyUser && typeof showToast === 'function') {
          showToast(`☁️ Cloud Sync (Offline cache active - ${this.bills.length} bills preserved)`, 'info');
        }
        this.notifyRemoteBillUpdate(null, false);
        return true;
      }

      try {
        // 1. Fetch permanently deleted bills tombstone collection first
        try {
          const delSnapshot = await this.firestore.collection('nwr_deleted_bills').get();
          delSnapshot.forEach(doc => {
            this.markBillPermanentlyDeleted(doc.id);
          });
        } catch (e) {
          console.warn('Note on nwr_deleted_bills:', e.message);
        }

        // 2. Filter local bills to eliminate any that were permanently deleted
        this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));

        const snapshot = await this.firestore.collection('nwr_bills').get();
        const remoteBills = [];
        snapshot.forEach(doc => {
          const d = doc.data();
          if (!d || !d.id) return;

          if (this.isBillDeleted(d.id, d.billNo) || d.deleted) {
            // Document was deleted; hard delete from nwr_bills if lingering
            this.firestore.collection('nwr_bills').doc(d.id).delete().catch(() => {});
          } else {
            remoteBills.push(d);
          }
        });

        // Two-way merge
        const localMap = new Map();
        this.bills.forEach(b => localMap.set(b.id, b));

        // 1. Merge remote bills into local
        remoteBills.forEach(rBill => {
          if (this.isBillDeleted(rBill.id, rBill.billNo)) return;
          if (!localMap.has(rBill.id)) {
            this.bills.unshift(rBill);
            localMap.set(rBill.id, rBill);
          } else {
            const lBill = localMap.get(rBill.id);
            const lTime = new Date(lBill.updatedAt || lBill.createdAt || 0).getTime();
            const rTime = new Date(rBill.updatedAt || rBill.createdAt || 0).getTime();
            if (rTime > lTime) {
              const idx = this.bills.findIndex(b => b.id === rBill.id);
              if (idx >= 0) this.bills[idx] = rBill;
            }
          }
        });

        // 2. Upload any local bills that remote didn't have (ONLY non-deleted)
        const remoteIdSet = new Set(remoteBills.map(b => b.id));
        const missingOnRemote = this.bills.filter(b => !remoteIdSet.has(b.id) && !this.isBillDeleted(b.id, b.billNo));

        for (const localOnlyBill of missingOnRemote) {
          await this.pushBillToCloud(localOnlyBill);
        }

        this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
        this.syncToLocalStorage();
        await this.persistAllBillsToIndexedDb(this.bills);

        this.isCloudConnected = true;
        this.log(`Cloud Sync completed: ${this.bills.length} active bills merged, ${missingOnRemote.length} uploaded.`, 'success');
        if (notifyUser && typeof showToast === 'function') {
          showToast(`☁️ Two-way Cloud Sync complete. ${this.bills.length} bills synchronized.`, 'success');
        }
        this.notifyRemoteBillUpdate(null, false);
        return true;
      } catch (err) {
        this.log(`Cloud Sync notice: ${err.message}`, 'warning');
        this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));
        this.syncToLocalStorage();
        await this.persistAllBillsToIndexedDb(this.bills);
        this.isCloudConnected = true;
        if (notifyUser && typeof showToast === 'function') {
          showToast(`☁️ Cloud Sync active (Offline cache - ${this.bills.length} bills synchronized)`, 'info');
        }
        this.notifyRemoteBillUpdate(null, false);
        return true;
      }
    } else {
      // Local mode
      if (notifyUser && typeof showToast === 'function') {
        showToast('App is currently in Local Browser Only mode.', 'info');
      }
      return true;
    }
  }

  // Reconcile two bill arrays by ID and timestamp
  reconcileLocalArrays(remoteBills) {
    if (!Array.isArray(remoteBills)) return;
    const localMap = new Map();
    this.bills.forEach((b, idx) => localMap.set(b.id, idx));

    remoteBills.forEach(rBill => {
      if (!rBill || !rBill.id) return;
      if (this.isBillDeleted(rBill.id, rBill.billNo)) return;

      if (localMap.has(rBill.id)) {
        const idx = localMap.get(rBill.id);
        const lBill = this.bills[idx];
        const lTime = new Date(lBill.updatedAt || lBill.createdAt || 0).getTime();
        const rTime = new Date(rBill.updatedAt || rBill.createdAt || 0).getTime();
        if (rTime > lTime) {
          this.bills[idx] = rBill;
        }
      } else {
        this.bills.unshift(rBill);
      }
    });

    // Filter out any permanently deleted bills
    this.bills = this.bills.filter(b => !this.isBillDeleted(b.id, b.billNo));

    // Sort descending by date/time
    this.bills.sort((a, b) => {
      const timeA = new Date(`${a.date || ''} ${a.time || ''}`).getTime() || 0;
      const timeB = new Date(`${b.date || ''} ${b.time || ''}`).getTime() || 0;
      return timeB - timeA;
    });
  }

  // ================= OFFLINE QUEUE DRAIN =================
  enqueueOfflineAction(action) {
    this.syncQueue.push(action);
    this.log(`Offline action queued (${this.syncQueue.length} pending).`, 'warning');
    this.notifyStatusChange();
  }

  async drainSyncQueue() {
    if (this.syncQueue.length === 0) return;
    this.log(`Draining ${this.syncQueue.length} offline actions...`, 'info');

    const queueCopy = [...this.syncQueue];
    this.syncQueue = [];

    for (const item of queueCopy) {
      try {
        if (item.type === 'SAVE_BILL') {
          if (item.bill && this.isBillDeleted(item.bill.id, item.bill.billNo)) {
            console.warn(`[QUEUE] Dropped queued SAVE_BILL for permanently deleted bill #${item.bill.billNo}`);
            continue;
          }
          if (this.currentMode === this.MODE_CLOUD) await this.pushBillToCloud(item.bill);
          if (this.currentMode === this.MODE_LAN) await this.pushBillToLan(item.bill);
        } else if (item.type === 'DELETE_BILL') {
          if (this.currentMode === this.MODE_CLOUD) await this.deleteBillFromCloud(item.billId);
          if (this.currentMode === this.MODE_LAN) await this.deleteBillFromLan(item.billId);
        }
      } catch (e) {
        console.warn('Failed to replay queued action:', e);
        this.syncQueue.push(item);
      }
    }
    this.notifyStatusChange();
  }

  onNetworkOnline() {
    if (this.currentMode === this.MODE_CLOUD) {
      this.initCloudMode();
    } else if (this.currentMode === this.MODE_LAN) {
      this.initLanMode();
    }
  }

  // ================= 1-CLICK BACKUP EXPORT & IMPORT =================
  exportBackupJson() {
    const backupData = {
      app: 'Shiv Sonography & Diagnostic Centre - Railway Portal',
      version: '2.0.0',
      exportedAt: new Date().toISOString(),
      workstationId: this.workstationId,
      workstationName: this.workstationName,
      totalBills: this.bills.length,
      config: this.config,
      catalog: this.catalog,
      bills: this.bills
    };

    const blob = new Blob([JSON.stringify(backupData, null, 2)], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const dateStr = new Date().toISOString().slice(0, 10);
    a.href = url;
    a.download = `shiv_diagnostic_nwr_backup_${dateStr}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    this.log(`Exported full backup (${this.bills.length} bills).`, 'success');
    if (typeof showToast === 'function') {
      showToast(`📥 Backup downloaded: ${this.bills.length} bills exported.`, 'success');
    }
  }

  async importBackupJson(fileOrJson, mergeMode = 'merge') {
    let data = null;
    if (typeof fileOrJson === 'string') {
      data = JSON.parse(fileOrJson);
    } else if (typeof fileOrJson === 'object') {
      data = fileOrJson;
    }

    if (!data || !Array.isArray(data.bills)) {
      throw new Error('Invalid backup file format. Expected a "bills" array.');
    }

    if (mergeMode === 'overwrite') {
      this.bills = [...data.bills];
    } else {
      this.reconcileLocalArrays(data.bills);
    }

    if (data.config) this.config = Object.assign({}, this.config, data.config);
    if (Array.isArray(data.catalog) && data.catalog.length > 0) this.catalog = [...data.catalog];

    this.syncToLocalStorage();
    await this.persistAllBillsToIndexedDb(this.bills);

    // Propagate all imported records to active cloud or LAN mode
    if (this.currentMode !== this.MODE_LOCAL) {
      await this.forceSyncAll(false);
    }

    this.log(`Imported backup: ${data.bills.length} bills loaded (${mergeMode} mode).`, 'success');
    if (typeof showToast === 'function') {
      showToast(`📤 Backup imported: ${this.bills.length} total records now in archive.`, 'success');
    }

    this.notifyRemoteBillUpdate(null, false);
    return this.bills.length;
  }

  // ================= DIAGNOSTICS & TEST CONNECTIONS =================
  async testLanConnection(customUrl = null) {
    const url = customUrl ? customUrl.trim().replace(/\/$/, '') : this.lanServerUrl;
    try {
      const start = Date.now();
      const res = await fetch(`${url}/api/health`, { method: 'GET', signal: AbortSignal.timeout(3000) });
      const latency = Date.now() - start;
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const info = await res.json();
      return {
        success: true,
        latency,
        serverTime: info.serverTime,
        connectedCount: info.connectedCount,
        totalBills: info.totalBills,
        activeWorkstations: info.activeWorkstations || []
      };
    } catch (err) {
      return {
        success: false,
        error: err.message
      };
    }
  }

  async testCloudConnection(customConfig = null) {
    const cfg = customConfig || this.firebaseConfig || DEFAULT_FIREBASE_CONFIG;
    if (!cfg || !cfg.projectId) {
      return { success: false, error: 'Firebase configuration is incomplete' };
    }
    if (!window.firebase) {
      return { success: false, error: 'Firebase SDK not loaded in browser' };
    }

    try {
      let testApp;
      const testName = 'testConnectionApp_' + Date.now();
      try {
        testApp = firebase.initializeApp(cfg, testName);
      } catch (e) {
        testApp = firebase.app(testName);
      }
      const testFs = testApp.firestore();
      
      const getPromise = testFs.collection('nwr_bills').limit(1).get();
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error('Cloud Firestore ping timed out (Local cache active)')), 4000)
      );
      
      await Promise.race([getPromise, timeoutPromise]);
      try { await testApp.delete(); } catch (e) {}
      return { success: true };
    } catch (err) {
      return { success: false, error: err.message || 'Firebase connection warning' };
    }
  }

  // ================= STATUS & UI BADGES =================
  getStatusInfo() {
    let text = '';
    let badgeClass = '';
    let icon = '';

    if (this.currentMode === this.MODE_CLOUD) {
      icon = '☁️';
      if (this.isCloudConnected) {
        text = 'Live Sync Active - Cloud';
        badgeClass = 'status-online-cloud';
      } else {
        text = 'Live Sync Active - Cloud (Offline Cache)';
        badgeClass = 'status-online-cloud';
      }
    } else if (this.currentMode === this.MODE_LAN) {
      icon = '🖧';
      if (this.isWsConnected) {
        const peers = this.activeWorkstations.length;
        text = `Live Sync Active - LAN (${peers} PC${peers === 1 ? '' : 's'})`;
        badgeClass = 'status-online-lan';
      } else {
        text = 'LAN Reconnecting...';
        badgeClass = 'status-warning';
      }
    } else {
      icon = '💾';
      text = 'Local Browser Only';
      badgeClass = 'status-local';
    }

    return {
      mode: this.currentMode,
      icon,
      text,
      badgeClass,
      workstationId: this.workstationId,
      workstationName: this.workstationName,
      pendingQueueCount: this.syncQueue.length,
      connectedPeers: this.activeWorkstations
    };
  }

  updateStatusBadge() {
    if (typeof document === 'undefined') return;
    const info = this.getStatusInfo();
    const badgeEl = document.getElementById('syncStatusBadge');
    const textEl = document.getElementById('syncStatusText');
    const dotEl = document.getElementById('syncStatusDot');

    if (badgeEl) {
      badgeEl.className = `sync-status-badge ${info.badgeClass}`;
    }
    if (textEl) {
      textEl.innerText = `${info.icon} ${info.text}`;
    }
    if (dotEl) {
      dotEl.className = `status-dot ${info.badgeClass}`;
    }

    const wsBadge = document.getElementById('syncWorkstationBadge');
    if (wsBadge) {
      wsBadge.innerText = `💻 ${this.workstationId}`;
      wsBadge.title = `Workstation: ${this.workstationName} (${this.workstationId})`;
    }

    // Update select dropdown in top nav if present
    const selectEl = document.getElementById('syncModeSelector');
    if (selectEl && selectEl.value !== this.currentMode) {
      selectEl.value = this.currentMode;
    }
  }

  // ================= EVENT SUBSCRIPTIONS & LOGGING =================
  onStatusChange(cb) { this.listeners.statusChange.push(cb); }
  onRemoteBillUpdate(cb) { this.listeners.remoteBillUpdate.push(cb); }
  onRemoteBillDelete(cb) { this.listeners.remoteBillDelete.push(cb); }
  onRemoteBillSoftDelete(cb) { this.listeners.remoteSoftDelete.push(cb); }
  onPeersChange(cb) { this.listeners.peersChange.push(cb); }
  onLog(cb) { this.listeners.logMessage.push(cb); }

  notifyStatusChange() {
    this.updateStatusBadge();
    const info = this.getStatusInfo();
    this.listeners.statusChange.forEach(cb => cb(info));
  }

  notifyRemoteBillUpdate(bill, isNew) {
    this.listeners.remoteBillUpdate.forEach(cb => cb(bill, isNew));
  }

  notifyRemoteBillSoftDelete(bill) {
    this.listeners.remoteSoftDelete.forEach(cb => cb(bill));
  }

  notifyRemoteBillDelete(billId) {
    this.listeners.remoteBillDelete.forEach(cb => cb(billId));
  }

  getActiveBills() {
    return this.bills.filter(b => !b.isDeleted && b.status !== 'inactive');
  }

  getInactiveBills() {
    return this.bills.filter(b => b.isDeleted || b.status === 'inactive');
  }

  notifyPeersChange() {
    this.notifyStatusChange();
    this.listeners.peersChange.forEach(cb => cb(this.activeWorkstations));
  }

  log(message, level = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    const entry = { timestamp, message, level };
    this.activityLogs.unshift(entry);
    if (this.activityLogs.length > 100) this.activityLogs.pop();
    
    // Print to developer console
    const prefix = `[SYNC-${level.toUpperCase()}]`;
    if (level === 'error') console.error(prefix, message);
    else if (level === 'warning') console.warn(prefix, message);
    else console.log(prefix, message);

    this.listeners.logMessage.forEach(cb => cb(entry));
  }
}

// Global instance creation
if (typeof window !== 'undefined') {
  window.syncManager = new DualModeSyncManager();
}
if (typeof module !== 'undefined' && module.exports) {
  module.exports = DualModeSyncManager;
}
