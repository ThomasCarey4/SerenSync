const net = require('net');

module.exports = function (app) {
  let plugin = {};
  let deltaStreamUnsubscribe = null;
  let socketClient = null;
  let reconnectTimer = null;
  let isShuttingDown = false;
  
  const SOCKET_PATH = '/tmp/signalk.sock';
  const RECONNECT_INTERVAL = 5000; // 5 seconds

  /**
   * Plugin metadata
   */
  plugin.id = 'seren-sync';
  plugin.name = 'Seren Sync';
  plugin.description = 'Forwards SignalK delta data to Unix domain socket with JSON transformation';
  plugin.schema = {
    type: 'object',
    properties: {}
  };

  /**
   * Start the plugin
   */
  plugin.start = function (options) {
    app.debug('Starting SignalK Data Forwarder plugin');
    
    // Initialize socket connection
    connectToSocket();
    
    // Subscribe to SignalK delta stream
    subscribeToDeltas();
    
    app.debug('SignalK Data Forwarder plugin started successfully');
  };

  /**
   * Stop the plugin
   */
  plugin.stop = function () {
    app.debug('Stopping SignalK Data Forwarder plugin');
    
    isShuttingDown = true;
    
    // Clear reconnection timer
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    
    // Unsubscribe from delta stream
    if (deltaStreamUnsubscribe) {
      deltaStreamUnsubscribe();
      deltaStreamUnsubscribe = null;
      app.debug('Unsubscribed from delta stream');
    }
    
    // Close socket connection
    if (socketClient) {
      socketClient.destroy();
      socketClient = null;
      app.debug('Socket connection closed');
    }
    
    app.debug('SignalK Data Forwarder plugin stopped');
  };

  /**
   * Connect to the Unix domain socket
   */
  function connectToSocket() {
    if (isShuttingDown) return;

    app.debug('Attempting to connect to socket at', SOCKET_PATH);

    // Use net.createConnection for a more robust connection
    socketClient = net.createConnection({ path: SOCKET_PATH });

    // 'connect' event is fired on successful connection
    socketClient.on('connect', () => {
      app.debug('Successfully connected to Unix domain socket at ' + SOCKET_PATH);
      // writeToSocket({test: true}); // Add this line for testing
    });

    // 'error' event handles any connection or stream errors
    socketClient.on('error', (error) => {
      app.error('Socket connection error:', error.message);
      // No need to call scheduleReconnection() here, the 'close' event will handle it.
    });

    // 'close' event is always fired when a socket is closed, either by error or normally
    socketClient.on('close', (hadError) => {
      app.debug(`Socket connection closed. HadError: ${hadError}`);
      if (!isShuttingDown) {
        scheduleReconnection();
      }
    });
  }

  /**
   * Schedule a reconnection attempt
   */
  function scheduleReconnection() {
    if (isShuttingDown || reconnectTimer) return;
    
    app.debug('Scheduling socket reconnection in ' + (RECONNECT_INTERVAL / 1000) + ' seconds');
    
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!isShuttingDown) {
        app.debug('Attempting to reconnect to socket');
        connectToSocket();
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
      deltaStreamUnsubscribe = () => valueStream.offValue(processValueObj);
      app.debug('Successfully subscribed to value stream');
    } catch (error) {
      app.error('Failed to subscribe to value stream:', error.message);
    }
  }

  // Add this function:
  function processValueObj(valueObj) {
    app.debug('Received value:', JSON.stringify(valueObj));
    const { path, value, timestamp } = valueObj;
    const source = valueObj['$source'] || 'unknown';
    const transformedData = {
      path,
      time: timestamp,
      value,
      source
    };
    writeToSocket(transformedData);
  }

  /**
   * Process a delta message and transform it
   * @param {Object} delta - The SignalK delta message
   */
  function processDelta(delta) {
    app.debug('Received delta:', JSON.stringify(delta)); // Add this line
    try {
      // Validate delta structure
      if (!delta || !delta.updates || !Array.isArray(delta.updates)) {
        return;
      }

      // Process each update in the delta
      delta.updates.forEach(update => {
        processUpdate(update);
      });
      
    } catch (error) {
      app.error('Error processing delta:', error.message);
    }
  }

  /**
   * Process a single update from the delta
   * @param {Object} update - A single update object from the delta
   */
  function processUpdate(update) {
    app.debug('Processing update:', JSON.stringify(update)); // Add this line
    try {
      // Validate update structure
      if (!update || !update.values || !Array.isArray(update.values)) {
        return;
      }

      const timestamp = update.timestamp;
      const source = extractSourceLabel(update.source);

      // Process each value in the update
      update.values.forEach(valueObj => {
        const transformedData = transformValue(valueObj, timestamp, source);
        if (transformedData) {
          writeToSocket(transformedData);
        }
      });
      
    } catch (error) {
      app.error('Error processing update:', error.message);
    }
  }

  /**
   * Extract the source label from the source object
   * @param {Object} source - The source object from the update
   * @returns {string} The source label or 'unknown' if not found
   */
  function extractSourceLabel(source) {
    if (!source) return 'unknown';
    if (source.label && source.src) return `${source.label}.${source.src}`;
    if (source.label) return source.label;
    if (source.type) return source.type;
    return 'unknown';
  }

  /**
   * Transform a value object into the required output format
   * @param {Object} valueObj - The value object containing path and value
   * @param {string} timestamp - The timestamp from the update
   * @param {string} source - The source label
   * @returns {Object|null} The transformed data object or null if invalid
   */
  function transformValue(valueObj, timestamp, source) {
    app.debug('Transforming value:', JSON.stringify(valueObj)); // Add this line
    try {
      // Validate value object structure
      if (!valueObj || !valueObj.path || valueObj.value === undefined) {
        return null;
      }

      return {
        path: valueObj.path,
        time: timestamp,
        value: valueObj.value,
        source: source
      };
      
    } catch (error) {
      app.error('Error transforming value:', error.message);
      return null;
    }
  }

  /**
   * Write transformed data to the Unix domain socket
   * @param {Object} data - The transformed data object
   */
  function writeToSocket(data) {
    try {
      // Check if socket is connected
      if (!socketClient || socketClient.destroyed || !socketClient.writable) {
        app.debug('Socket not available, skipping data write');
        return;
      }

      // Serialize data to JSON string with newline terminator
      const jsonString = JSON.stringify(data) + '\n';
      app.debug('Writing to socket:', jsonString);

      // Write to socket
      socketClient.write(jsonString, 'utf8', (error) => {
        if (error) {
          app.error('Error writing to socket:', error.message);
        }
      });
      
    } catch (error) {
      app.error('Error writing to socket:', error.message);
    }
  }

  return plugin;
};
