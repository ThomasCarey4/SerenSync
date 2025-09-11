const net = require('net');

module.exports = function (app) {
  let plugin = {};
  let deltaStreamUnsubscribe = null;
  let socketClients = {};
  let reconnectTimers = {};
  let isShuttingDown = false;
  let lastTransmissionTimes = {}; // Track per-path throttling
  
  // Socket configuration mapping categories to socket paths
  const SOCKET_CONFIG = {
    sensor: '/tmp/signalk_sensors.sock',
    position: '/tmp/signalk_position.sock',
    state: '/tmp/signalk_state.sock'
  };
  
  const RECONNECT_INTERVAL = 5000; // 5 seconds
  
  // Category classification regex patterns
  const CATEGORY_PATTERNS = {
    dump: [
      /^(.*)(totalPanel)(.*)$/,
      /^(.*)(ModeNumber)(.*)$/,
      /^(.*)(consumedCharge)(.*)$/,
      /^(electrical\.batteries.279)(.*)$/
    ],
    state: [
      /^(.*)((m|M)ode)$/,
      /^(.*)\.(name)$/,
      /^(.*)\.(state)$/,
      /^(electrical\.batteries\.)([0-9]+)(\.capacity\.dischargedEnergy)$/,
      /^(electrical\.batteries\.)([0-9]+)(\.capacity\.stateOfCharge)$/,
      /^(electrical\.batteries\.)([0-9]+)(\.lifetimeDischarge)$/,
      /^(electrical\.solar\.)([0-9]+)(\.systemYield)$/,
      /^(electrical\.solar\.)([0-9]+)(\.yieldToday)$/,
      /^(electrical\.solar\.)([0-9]+)(\.yieldYesterday)$/,
      /^(navigation\.courseRhumbline\.nextPoint\.position)$/,
      /^(navigation\.currentRoute\.)(.*)$/,
      /^(navigation\.gns)(.*)$/,
      /^(navigation\.magneticVariation)$/,
      /^(navigation\.trip\.log)$/,
      /^(notifications)(.*)$/,
      /^(sensors\.ais)(.*)$/,
      /^(steering)(.*)$/
    ],
    position: [
      /^(navigation\.position)$/
    ],
    sensor: [
      /^(.*)([cC]urrent)$/,
      /^(.*)([pP]ower)$/,
      /^(.*)([vV]oltage)$/,
      /^(.*)([tT]emperature)$/,
      /^(environment)(.*)$/,
      /^(navigation)(.*)$/
    ]
  };

  /**
   * Plugin metadata
   */
  plugin.id = 'seren-sync';
  plugin.name = 'Seren Sync';
  plugin.description = 'Forwards categorized SignalK delta data to multiple Unix domain sockets with compact serialization';
  plugin.schema = {
    type: 'object',
    properties: {
      positionSampleRate: {
        type: 'number',
        title: 'Position Sample Rate (ms)',
        description: 'Minimum interval between position data transmissions per path',
        default: 1000,
        minimum: 100
      },
      sensorSampleRate: {
        type: 'number',
        title: 'Sensor Sample Rate (ms)', 
        description: 'Minimum interval between sensor data transmissions per path',
        default: 2000,
        minimum: 100
      },
      stateSampleRate: {
        type: 'number',
        title: 'State Sample Rate (ms)',
        description: 'Minimum interval between state data transmissions per path', 
        default: 500,
        minimum: 100
      }
    }
  };

  /**
   * Start the plugin
   */
  plugin.start = function (options) {
    app.debug('Starting SignalK Categorized Data Forwarder plugin');
    
    // Store configuration options
    plugin.options = options || {};
    
    // Initialize socket connections for each category
    Object.keys(SOCKET_CONFIG).forEach(category => {
      connectToSocket(category);
    });
    
    // Subscribe to SignalK delta stream
    subscribeToDeltas();
    
    app.debug('SignalK Categorized Data Forwarder plugin started successfully');
  };

  /**
   * Stop the plugin
   */
  plugin.stop = function () {
    app.debug('Stopping SignalK Categorized Data Forwarder plugin');
    
    isShuttingDown = true;
    
    // Clear all reconnection timers
    Object.keys(reconnectTimers).forEach(category => {
      if (reconnectTimers[category]) {
        clearTimeout(reconnectTimers[category]);
        reconnectTimers[category] = null;
      }
    });
    
    // Unsubscribe from delta stream
    if (deltaStreamUnsubscribe) {
      deltaStreamUnsubscribe();
      deltaStreamUnsubscribe = null;
      app.debug('Unsubscribed from delta stream');
    }
    
    // Close all socket connections
    Object.keys(socketClients).forEach(category => {
      if (socketClients[category]) {
        socketClients[category].destroy();
        socketClients[category] = null;
        app.debug(`Socket connection closed for category: ${category}`);
      }
    });
    
    // Clear transmission tracking
    lastTransmissionTimes = {};
    
    app.debug('SignalK Categorized Data Forwarder plugin stopped');
  };

  /**
   * Connect to a Unix domain socket for a specific category
   * @param {string} category - The data category (sensor, position, state)
   */
  function connectToSocket(category) {
    if (isShuttingDown) return;

    const socketPath = SOCKET_CONFIG[category];
    app.debug(`Attempting to connect to ${category} socket at ${socketPath}`);

    // Use net.createConnection for a robust connection
    socketClients[category] = net.createConnection({ path: socketPath });

    // 'connect' event is fired on successful connection
    socketClients[category].on('connect', () => {
      app.debug(`Successfully connected to ${category} Unix domain socket at ${socketPath}`);
    });

    // 'error' event handles any connection or stream errors
    socketClients[category].on('error', (error) => {
      app.error(`Socket connection error for ${category}:`, error.message);
    });

    // 'close' event is always fired when a socket is closed
    socketClients[category].on('close', (hadError) => {
      app.debug(`${category} socket connection closed. HadError: ${hadError}`);
      if (!isShuttingDown) {
        scheduleReconnection(category);
      }
    });
  }

  /**
   * Schedule a reconnection attempt for a specific category
   * @param {string} category - The data category to reconnect
   */
  function scheduleReconnection(category) {
    if (isShuttingDown || reconnectTimers[category]) return;
    
    app.debug(`Scheduling ${category} socket reconnection in ${RECONNECT_INTERVAL / 1000} seconds`);
    
    reconnectTimers[category] = setTimeout(() => {
      reconnectTimers[category] = null;
      if (!isShuttingDown) {
        app.debug(`Attempting to reconnect to ${category} socket`);
        connectToSocket(category);
      }
    }, RECONNECT_INTERVAL);
  }

  /**
   * Subscribe to SignalK delta stream
   */
  function subscribeToDeltas() {
    try {
      const valueStream = app.streambundle.getSelfBus();
      valueStream.onValue(processValueObj);
      deltaStreamUnsubscribe = () => valueStream.off(processValueObj);
      app.debug('Successfully subscribed to value stream');
    } catch (error) {
      app.error('Failed to subscribe to value stream:', error.message);
    }
  }

  /**
   * Process a value object from the SignalK stream
   * @param {Object} valueObj - The value object containing path, value, timestamp, etc.
   */
  function processValueObj(valueObj) {
    try {
      const { path, value, timestamp } = valueObj;
      
      // Validate essential data
      if (!path || value === undefined || !timestamp) {
        return;
      }

      // Categorize the data based on path
      const category = categorizeData(path);
      
      // Discard 'dump' category data immediately
      if (category === 'dump') {
        app.debug(`Discarding dump category data for path: ${path}`);
        return;
      }

      if (
        category === 'position' &&
        value &&
        typeof value === 'object' &&
        (
          Number(value.latitude) === 0 ||
          Number(value.longitude) === 0
        )
      ) {
        app.debug(`Discarding position with zero lat/lon`);
        return;
      }

      // Check throttling for this specific path
      if (!shouldTransmit(path, category, timestamp)) {
        // app.debug(`Throttled data for path: ${path}`);
        return;
      }

      // Update last transmission time for this path
      lastTransmissionTimes[path] = Date.now();

      // Extract source information
      const source = valueObj['$source'] || 'unknown';
      let timestampMs;
      if (typeof timestamp === 'string') {
        timestampMs = Math.floor(new Date(timestamp).getTime());
      } else if (typeof timestamp === 'number') {
        // Assume it's already in milliseconds if > 1e12, otherwise convert from seconds
        timestampMs = timestamp > 1e12 ? Math.floor(timestamp) : Math.floor(timestamp * 1000);
      } else {
        timestampMs = Date.now(); // Fallback to current time
      }

      // Create measurement data
      const measurementData = {
        path,
        time: timestampMs,
        value,
        source
      };

      // Serialize and write to appropriate socket
      writeToSocket(category, measurementData);
      
    } catch (error) {
      app.error('Error processing value object:', error.message);
    }
  }

  /**
   * Categorize data based on the Signal K path
   * @param {string} path - The Signal K path
   * @returns {string} The category ('position', 'sensor', 'state', or 'dump')
   */
  function categorizeData(path) {
    // Test against each category's array of patterns
    for (const pattern of CATEGORY_PATTERNS.dump) {
      if (pattern.test(path)) {
        return 'dump';
      }
    }
    for (const [category, patterns] of Object.entries(CATEGORY_PATTERNS)) {
      if (category === 'dump') continue;
      for (const pattern of patterns) {
        if (pattern.test(path)) {
          return category;
        }
      }
    }
    // Default to 'dump' if no pattern matches
    return 'dump';
  }

  /**
   * Determine if data should be transmitted based on per-path throttling
   * @param {string} path - The Signal K path
   * @param {string} category - The data category
   * @param {string} timestamp - The current timestamp
   * @returns {boolean} True if data should be transmitted
   */
  function shouldTransmit(path, category, timestamp) {
    const now = Date.now();
    const lastTransmissionTime = lastTransmissionTimes[path] || 0;
    
    // Get the sample rate for this category from options
    let sampleRate;
    switch (category) {
      case 'position':
        sampleRate = plugin.options.positionSampleRate || 1000;
        break;
      case 'sensor':
        sampleRate = plugin.options.sensorSampleRate || 2000;
        break;
      case 'state':
        sampleRate = plugin.options.stateSampleRate || 500;
        break;
      default:
        sampleRate = 1000; // Default fallback
    }
    
    // Check if enough time has elapsed since last transmission for this path
    return (now - lastTransmissionTime) >= sampleRate;
  }

  /**
   * Serialize measurement data into a highly compact format
   * 
   * COMPACT SERIALIZATION FORMAT:
   * Each line contains: [timestamp_ms]|[path]|[source]|[value]
   * - timestamp_ms: Unix timestamp in milliseconds (13 digits)
   * - path: Full Signal K path (string)
   * - source: Source identifier (string, pipe characters escaped)
   * - value: The measurement value (number, string, boolean, or object as JSON)
   * - Delimiter: Single pipe character '|'
   * - Terminator: Single newline character '\n'
   * 
   * Examples:
   * 1694458123456|navigation.speedOverGround|gps.0|12.3
   * 1694458123457|environment.outside.temperature|onewire.28FF123456|285.15
   * 1694458123458|notifications.navigation.gnss|nmea.0|{"state":"normal","message":"GPS fix OK"}
   * 
   * This format minimizes overhead while maintaining human readability for debugging.
   * Each message is self-contained on a single line for easy parsing by downstream consumers.
   * The source field preserves data provenance which is crucial for Signal K applications.
   * 
   * @param {Object} data - The measurement data object
   * @returns {string} The serialized data string
   */
  function serializeData(data) {
    try {
      // Convert timestamp to milliseconds if it's in seconds
      let timestampMs;
      if (typeof data.timestamp === 'string') {
        timestampMs = Math.floor(new Date(data.timestamp).getTime());
      } else if (typeof data.timestamp === 'number') {
        // Assume it's already in milliseconds if > 1e12, otherwise convert from seconds
        timestampMs = data.timestamp > 1e12 ? Math.floor(data.timestamp) : Math.floor(data.timestamp * 1000);
      } else {
        timestampMs = Date.now(); // Fallback to current time
      }

      // Escape pipe characters in source to prevent parsing issues
      const escapedSource = (data.source || 'unknown').replace(/\|/g, '\\|');

      // Serialize value based on its type
      let serializedValue;
      if (typeof data.value === 'object' && data.value !== null) {
        // For objects, use compact JSON representation
        serializedValue = JSON.stringify(data.value);
      } else if (typeof data.value === 'string') {
        // For strings, escape any pipe characters to prevent parsing issues
        serializedValue = data.value.replace(/\|/g, '\\|');
      } else {
        // For numbers, booleans, etc., convert to string
        serializedValue = String(data.value);
      }

      // Construct the compact format: timestamp|path|source|value
      const serialized = `${timestampMs}|${data.path}|${escapedSource}|${serializedValue}\n`;
      
      app.debug(`Serialized data: ${serialized.trim()}`);
      return serialized;
      
    } catch (error) {
      app.error('Error serializing data:', error.message);
      return null;
    }
  }

  /**
   * Write serialized data to the appropriate category socket
   * @param {string} category - The data category
   * @param {Object} data - The measurement data object
   */
  function writeToSocket(category, data) {
    try {
      const socketClient = socketClients[category];
      
      // Check if socket is connected and writable
      if (!socketClient || socketClient.destroyed || !socketClient.writable) {
        app.debug(`Socket not available for category ${category}, skipping data write`);
        return;
      }

      // Serialize the data into compact format
      // const serializedData = serializeData(data);
      // if (!serializedData) {
      //   app.error(`Failed to serialize data for category ${category}`);
      //   return;
      // }

      // // Write to socket
      // socketClient.write(serializedData, 'utf8', (error) => {
      //   if (error) {
      //     app.error(`Error writing to ${category} socket:`, error.message);
      //   } else {
      //     app.debug(`Successfully wrote to ${category} socket: ${serializedData.trim()}`);
      //   }
      // });

      const jsonString = JSON.stringify(data) + '\n';
      app.debug('Writing to socket:', jsonString);

      // Write to socket
      socketClient.write(jsonString, 'utf8', (error) => {
        if (error) {
          app.error('Error writing to socket:', error.message);
        }
      });
      
    } catch (error) {
      app.error(`Error writing to ${category} socket:`, error.message);
    }
  }

  return plugin;
};
