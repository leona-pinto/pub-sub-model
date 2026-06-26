const socket = io();

function updateLocation(deviceState) {
  const config = SMART_HOME_MODULAR_CONFIG.location;
  const { carDistance, carLatitude, carLongitude } = deviceState;

  document.getElementById(config.latitudeId).textContent =
    carLatitude !== null ? carLatitude.toFixed(4) : '--';

  document.getElementById(config.longitudeId).textContent =
    carLongitude !== null ? carLongitude.toFixed(4) : '--';

  document.getElementById(config.distanceValueId).textContent =
    carDistance !== null ? carDistance.toFixed(2) : '--';
}

function updateTemperature(deviceState) {
  const config = SMART_HOME_MODULAR_CONFIG.location;
  const { currentTemp } = deviceState;

  document.getElementById(config.temperatureValueId).textContent =
    currentTemp !== null ? currentTemp.toFixed(2) + ' °C' : '-- °C';
}

// Generic device update function
 
function updateDevice(deviceKey, deviceState) {
  const deviceConfig = SMART_HOME_MODULAR_CONFIG.devices[deviceKey];

  if (!deviceConfig) {
    console.warn(`Device config not found for: ${deviceKey}`);
    return;
  }

  const statusElement = document.getElementById(deviceConfig.statusId);
  const textElement = document.getElementById(deviceConfig.textId);

  if (!statusElement || !textElement) {
    console.warn(`DOM elements not found for device: ${deviceKey}`);
    return;
  }

  // Call device-specific update logic from config
  deviceConfig.update(statusElement, textElement, deviceState);
}

function updateAllDevices(deviceState) {
  Object.keys(SMART_HOME_MODULAR_CONFIG.devices).forEach(deviceKey => {
    updateDevice(deviceKey, deviceState.devices[deviceKey]);
  });
}

/**
 * Socket.io connection handler
 */
socket.on('connect', () => {
  console.log('Connected to Smart Home server (Modular)');
});

/**
 * Socket.io disconnection handler
 */
socket.on('disconnect', () => {
  console.log('Disconnected from Smart Home server (Modular)');
});

socket.on('device-state-update', (deviceState) => {
  console.log('Device state update:', deviceState);

  // Update shared location/temperature
  updateLocation(deviceState);
  updateTemperature(deviceState);

  // Update all devices using config
  updateAllDevices(deviceState);
});

console.log('Smart Home Dashboard (Modular) Loaded');
console.log('Active devices:', Object.keys(SMART_HOME_MODULAR_CONFIG.devices));
