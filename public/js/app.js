const socket = io();

// Update location data
function updateLocation(deviceState) {
  const { carDistance, carLatitude, carLongitude, lastUpdate } = deviceState;

  // Update coordinates
  document.getElementById('latitude').textContent = carLatitude !== null ? carLatitude.toFixed(4) : '--';
  document.getElementById('longitude').textContent = carLongitude !== null ? carLongitude.toFixed(4) : '--';
  document.getElementById('distance-value').textContent = carDistance !== null ? carDistance.toFixed(2) : '--';

  // Update last update time
  if (lastUpdate) {
    const date = new Date(lastUpdate);
    const time = date.toLocaleTimeString();
    document.getElementById('last-update').textContent = `Last update: ${time}`;
  }
}

// Update HVAC device
function updateHvac(hvacDevice) {
  const hvacStatus = document.getElementById('hvac-status');
  const hvacIcon = document.getElementById('hvac-icon');
  const hvacText = document.getElementById('hvac-text');
  const hvacMode = document.getElementById('hvac-mode');
  const hvacModeIcon = document.getElementById('hvac-mode-icon');
  const hvacModeText = document.getElementById('hvac-mode-text');

  if (hvacDevice.isPowerOn) {
    hvacStatus.classList.remove('status-off');
    hvacStatus.classList.add('status-on');
    hvacText.textContent = 'ON';

    // Show mode
    hvacMode.style.display = 'flex';
    if (hvacDevice.mode === 'HEATING') {
      hvacModeIcon.classList.remove('cooling');
      hvacModeIcon.classList.add('heating');
      hvacModeText.textContent = 'Heating';
      hvacMode.classList.add('heating');
      hvacMode.classList.remove('cooling');
    } else if (hvacDevice.mode === 'COOLING') {
      hvacModeIcon.classList.remove('heating');
      hvacModeIcon.classList.add('cooling');
      hvacModeText.textContent = 'Cooling';
      hvacMode.classList.add('cooling');
      hvacMode.classList.remove('heating');
    }
  } else {
    hvacStatus.classList.remove('status-on');
    hvacStatus.classList.add('status-off');
    hvacText.textContent = 'OFF';
    hvacMode.style.display = 'none';
  }
}

// Update Smart TV device
function updateTv(tvDevice) {
  const tvStatus = document.getElementById('tv-status');
  const tvIcon = document.getElementById('tv-icon');
  const tvText = document.getElementById('tv-text');

  if (tvDevice.isPowerOn) {
    tvStatus.classList.remove('status-off');
    tvStatus.classList.add('status-on');
    tvText.textContent = 'ON';
  } else {
    tvStatus.classList.remove('status-on');
    tvStatus.classList.add('status-off');
    tvText.textContent = 'OFF';
  }
}

// Update Barbecue device
function updateBarbecue(barbecueDevice) {
  const barbecueStatus = document.getElementById('barbecue-status');
  const barbecueIcon = document.getElementById('barbecue-icon');
  const barbecueText = document.getElementById('barbecue-text');

  if (barbecueDevice.isPowerOn) {
    barbecueStatus.classList.remove('status-off');
    barbecueStatus.classList.add('status-on');
    barbecueText.textContent = 'ON';
  } else {
    barbecueStatus.classList.remove('status-on');
    barbecueStatus.classList.add('status-off');
    barbecueText.textContent = 'OFF';
  }
}

// Socket.io event handlers
socket.on('connect', () => {
  console.log('Connected to server');
  document.getElementById('connection-status').classList.add('connected');
  document.getElementById('connection-status').classList.remove('disconnected');
  document.getElementById('connection-status').textContent = 'Connected';
});

socket.on('disconnect', () => {
  console.log('Disconnected from server');
  document.getElementById('connection-status').classList.add('disconnected');
  document.getElementById('connection-status').classList.remove('connected');
  document.getElementById('connection-status').textContent = 'Disconnected';
});

socket.on('device-state-update', (deviceState) => {
  console.log('Device state update:', deviceState);

  // Update location data
  updateLocation(deviceState);

  // Update all devices
  updateHvac(deviceState.devices.hvac);
  updateTv(deviceState.devices.tv);
  updateBarbecue(deviceState.devices.barbecue);
});

socket.on('error', (error) => {
  console.error('Socket error:', error);
});

console.log('Smart Home Dashboard Loaded');
