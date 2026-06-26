const socket = io();

// Update location card with lat, long, and distance
function updateLocation(deviceState) {
  const { carDistance, carLatitude, carLongitude, lastUpdate } = deviceState;

  document.getElementById('latitude').textContent = carLatitude !== null ? carLatitude.toFixed(4) : '--';
  document.getElementById('longitude').textContent = carLongitude !== null ? carLongitude.toFixed(4) : '--';
  document.getElementById('distance-value').textContent = carDistance !== null ? carDistance.toFixed(2) : '--';
}

// Update temperature card
function updateTemperature(deviceState) {
  const { currentTemp } = deviceState;
  document.getElementById('temperature-value').textContent = currentTemp !== null ? currentTemp.toFixed(2) + ' °C' : '-- °C';
}

// Update HVAC card with status and mode
function updateHvac(hvacDevice) {
  const hvacStatus = document.getElementById('hvac-status');
  const hvacText = document.getElementById('hvac-text');
  const hvacMode = document.getElementById('hvac-mode');
  const hvacModeText = document.getElementById('hvac-mode-text');

  if (hvacDevice.isPowerOn) {
    hvacStatus.classList.add('on');
    hvacStatus.classList.remove('off');
    hvacText.textContent = 'ON';

    // Show heating or cooling mode
    hvacMode.style.display = 'flex';
    if (hvacDevice.mode === 'HEATING') {
      hvacModeText.textContent = 'Heating';
    } else if (hvacDevice.mode === 'COOLING') {
      hvacModeText.textContent = 'Cooling';
    }
  } else {
    hvacStatus.classList.remove('on');
    hvacStatus.classList.add('off');
    hvacText.textContent = 'OFF';
    hvacMode.style.display = 'none';
  }
}

// Update Smart TV card with on/off status
function updateTv(tvDevice) {
  const tvStatus = document.getElementById('tv-status');
  const tvText = document.getElementById('tv-text');

  if (tvDevice.isPowerOn) {
    tvStatus.classList.add('on');
    tvStatus.classList.remove('off');
    tvText.textContent = 'ON';
  } else {
    tvStatus.classList.remove('on');
    tvStatus.classList.add('off');
    tvText.textContent = 'OFF';
  }
}

// Update Barbecue card with on/off status
function updateBarbecue(barbecueDevice) {
  const barbecueStatus = document.getElementById('barbecue-status');
  const barbecueText = document.getElementById('barbecue-text');

  if (barbecueDevice.isPowerOn) {
    barbecueStatus.classList.add('on');
    barbecueStatus.classList.remove('off');
    barbecueText.textContent = 'ON';
  } else {
    barbecueStatus.classList.remove('on');
    barbecueStatus.classList.add('off');
    barbecueText.textContent = 'OFF';
  }
}

// Update Smart Lights card with on/off status
function updateLights(lightsDevice) {
  const lightsStatus = document.getElementById('lights-status');
  const lightsText = document.getElementById('lights-text');

  if (lightsDevice.isPowerOn) {
    lightsStatus.classList.add('on');
    lightsStatus.classList.remove('off');
    lightsText.textContent = 'ON';
  } else {
    lightsStatus.classList.remove('on');
    lightsStatus.classList.add('off');
    lightsText.textContent = 'OFF';
  }
}

// Socket.io event handlers
socket.on('connect', () => {
  console.log('Connected to server');
});

socket.on('disconnect', () => {
  console.log('Disconnected from server');
});

// Handle device state updates
socket.on('device-state-update', (deviceState) => {
  console.log('Device state update:', deviceState);

  // Update all elements
  updateLocation(deviceState);
  updateTemperature(deviceState);
  updateHvac(deviceState.devices.hvac);
  updateTv(deviceState.devices.tv);
  updateBarbecue(deviceState.devices.barbecue);
  updateLights(deviceState.devices.lights);
});

console.log('Smart Home Dashboard (Flexible) Loaded');
