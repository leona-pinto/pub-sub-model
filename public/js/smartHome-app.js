const socket = io();

// Update location card with lat, long, and distance
function updateLocation(deviceState) {
  const { carDistance, carLatitude, carLongitude, lastUpdate } = deviceState;

  document.getElementById('latitude').textContent = carLatitude !== null ? carLatitude.toFixed(4) : '--';
  document.getElementById('longitude').textContent = carLongitude !== null ? carLongitude.toFixed(4) : '--';
  document.getElementById('distance-value').textContent = carDistance !== null ? carDistance.toFixed(2) : '--';
}

// Update temperature and humidity card
function updateTemperature(deviceState) {
  const { currentTemp } = deviceState;
  document.getElementById('temperature-value').textContent = currentTemp !== null ? currentTemp.toFixed(2) + ' °C' : '-- °C';
}

// Update humidity display
function updateHumidity(deviceState) {
  const { currentHumidity } = deviceState;
  document.getElementById('humidity-value').textContent = currentHumidity !== null ? currentHumidity.toFixed(1) + ' %' : '-- %';
}

// Update HVAC card status and humidifier
function updateHvac(hvacDevice) {
  const hvacStatus = document.getElementById('hvac-status');
  const hvacText = document.getElementById('hvac-text');

  if (hvacDevice.isPowerOn) {
    hvacStatus.classList.add('on');
    hvacStatus.classList.remove('off');
    hvacText.textContent = 'ON';
  } else {
    hvacStatus.classList.remove('on');
    hvacStatus.classList.add('off');
    hvacText.textContent = 'OFF';
  }

  // Update humidifier status
  if (hvacDevice.humidifier && hvacDevice.humidifier.isOn) {
    document.getElementById('humidifier-status').classList.add('on');
    document.getElementById('humidifier-status').classList.remove('off');
    document.getElementById('humidifier-text').textContent = 'ON';
  } else {
    document.getElementById('humidifier-status').classList.remove('on');
    document.getElementById('humidifier-status').classList.add('off');
    document.getElementById('humidifier-text').textContent = 'OFF';
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
  updateHumidity(deviceState);
  updateHvac(deviceState.devices.hvac);
  updateTv(deviceState.devices.tv);
  updateBarbecue(deviceState.devices.barbecue);
});

console.log('Smart Home Dashboard Loaded');
