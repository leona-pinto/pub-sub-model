const socket = io();

socket.on('device-state-update', (deviceState) => {
  updateDisplay(deviceState);
});

function updateDisplay(deviceState) {
  const hvac = deviceState.devices.hvac;

  // GPS data
  document.getElementById('latitude').textContent  = deviceState.carLatitude?.toFixed(4)  || '--';
  document.getElementById('longitude').textContent = deviceState.carLongitude?.toFixed(4) || '--';

  if (deviceState.carDistance !== null) {
    document.getElementById('distance').textContent = deviceState.carDistance.toFixed(2);
  }

  document.getElementById('temperature').textContent = deviceState.currentTemp?.toFixed(1) || '--';

  // HVAC Power
  const hvacPower = document.getElementById('hvac-power');
  if (hvac.isPowerOn) {
    hvacPower.classList.add('on');
    hvacPower.classList.remove('off');
    hvacPower.querySelector('.status-icon').textContent = '🟢';
    hvacPower.querySelector('.status-text').textContent = 'ON';
  } else {
    hvacPower.classList.remove('on');
    hvacPower.classList.add('off');
    hvacPower.querySelector('.status-icon').textContent = '⚪';
    hvacPower.querySelector('.status-text').textContent = 'OFF';
  }

  // Cooling
  const coolingStatus = document.getElementById('cooling-status');
  if (hvac.mode === 'COOLING') {
    coolingStatus.classList.add('on');
    coolingStatus.classList.remove('off');
    coolingStatus.querySelector('.status-icon').textContent = '🟦';
    coolingStatus.querySelector('.status-text').textContent = 'ON';
  } else {
    coolingStatus.classList.remove('on');
    coolingStatus.classList.add('off');
    coolingStatus.querySelector('.status-icon').textContent = '⚪';
    coolingStatus.querySelector('.status-text').textContent = 'OFF';
  }

  // Heating
  const heatingStatus = document.getElementById('heating-status');
  if (hvac.mode === 'HEATING') {
    heatingStatus.classList.add('on');
    heatingStatus.classList.remove('off');
    heatingStatus.querySelector('.status-icon').textContent = '🟥';
    heatingStatus.querySelector('.status-text').textContent = 'ON';
  } else {
    heatingStatus.classList.remove('on');
    heatingStatus.classList.add('off');
    heatingStatus.querySelector('.status-icon').textContent = '⚪';
    heatingStatus.querySelector('.status-text').textContent = 'OFF';
  }
}