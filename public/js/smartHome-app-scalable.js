const socket = io();

// Update location card
function updateLocation(deviceState) {
  const { carDistance, carLatitude, carLongitude, currentTemp } = deviceState;
  document.getElementById('latitude').textContent = carLatitude !== null ? carLatitude.toFixed(4) : '--';
  document.getElementById('longitude').textContent = carLongitude !== null ? carLongitude.toFixed(4) : '--';
  document.getElementById('distance-value').textContent = carDistance !== null ? carDistance.toFixed(2) : '--';
}

// Update temperature card
function updateTemperature(deviceState) {
  const { currentTemp } = deviceState;
  document.getElementById('temperature-value').textContent = currentTemp !== null ? currentTemp.toFixed(2) + ' °C' : '-- °C';
}

// Update HVAC
function updateHvac(hvacDevice) {
  const hvacStatus = document.getElementById('hvac-status');
  const hvacText = document.getElementById('hvac-text');
  const hvacMode = document.getElementById('hvac-mode');
  const hvacModeText = document.getElementById('hvac-mode-text');

  if (hvacDevice.isPowerOn) {
    hvacStatus.classList.add('on');
    hvacStatus.classList.remove('off');
    hvacText.textContent = 'ON';
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

// Update Smart TV
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

// Update Barbecue
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

// Update Fan
function updateFan(fanDevice) {
  const fanStatus = document.getElementById('fan-status');
  const fanText = document.getElementById('fan-text');

  if (fanDevice.isPowerOn) {
    fanStatus.classList.add('on');
    fanStatus.classList.remove('off');
    fanText.textContent = 'ON';
  } else {
    fanStatus.classList.remove('on');
    fanStatus.classList.add('off');
    fanText.textContent = 'OFF';
  }
}

// Update Air Purifier
function updateAirPurifier(airPurifierDevice) {
  const airPurifierStatus = document.getElementById('air-purifier-status');
  const airPurifierText = document.getElementById('air-purifier-text');

  if (airPurifierDevice.isPowerOn) {
    airPurifierStatus.classList.add('on');
    airPurifierStatus.classList.remove('off');
    airPurifierText.textContent = 'ON';
  } else {
    airPurifierStatus.classList.remove('on');
    airPurifierStatus.classList.add('off');
    airPurifierText.textContent = 'OFF';
  }
}

// Update Dryer
function updateDryer(dryerDevice) {
  const dryerStatus = document.getElementById('dryer-status');
  const dryerText = document.getElementById('dryer-text');

  if (dryerDevice.isPowerOn) {
    dryerStatus.classList.add('on');
    dryerStatus.classList.remove('off');
    dryerText.textContent = 'ON';
  } else {
    dryerStatus.classList.remove('on');
    dryerStatus.classList.add('off');
    dryerText.textContent = 'OFF';
  }
}

// Update Oven
function updateOven(ovenDevice) {
  const ovenStatus = document.getElementById('oven-status');
  const ovenText = document.getElementById('oven-text');

  if (ovenDevice.isPowerOn) {
    ovenStatus.classList.add('on');
    ovenStatus.classList.remove('off');
    ovenText.textContent = 'ON';
  } else {
    ovenStatus.classList.remove('on');
    ovenStatus.classList.add('off');
    ovenText.textContent = 'OFF';
  }
}

// Update Washing Machine
function updateWashingMachine(washingMachineDevice) {
  const washingMachineStatus = document.getElementById('washing-machine-status');
  const washingMachineText = document.getElementById('washing-machine-text');

  if (washingMachineDevice.isPowerOn) {
    washingMachineStatus.classList.add('on');
    washingMachineStatus.classList.remove('off');
    washingMachineText.textContent = 'ON';
  } else {
    washingMachineStatus.classList.remove('on');
    washingMachineStatus.classList.add('off');
    washingMachineText.textContent = 'OFF';
  }
}

// Socket.io event handlers
socket.on('connect', () => {
  console.log('Connected to server');
});

socket.on('disconnect', () => {
  console.log('Disconnected from server');
});

socket.on('device-state-update', (deviceState) => {
  console.log('Device state update:', deviceState);

  updateLocation(deviceState);
  updateTemperature(deviceState);
  updateHvac(deviceState.devices.hvac);
  updateTv(deviceState.devices.tv);
  updateBarbecue(deviceState.devices.barbecue);
  updateFan(deviceState.devices.fan);
  updateAirPurifier(deviceState.devices.airPurifier);
  updateDryer(deviceState.devices.dryer);
  updateOven(deviceState.devices.oven);
  updateWashingMachine(deviceState.devices.washingMachine);
});

console.log('Smart Home Dashboard (Scaling Test) Loaded');
