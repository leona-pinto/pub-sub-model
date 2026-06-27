const socket = io('http://localhost:4000');

function ts() {
  return new Date().toLocaleTimeString('en-GB', { hour12: false });
}

function addLog(type, tag, msg) {
  const log = document.getElementById('log');
  const line = document.createElement('div');
  line.className = 'log-line ' + type;
  line.innerHTML = `<span class="log-time">${ts()}</span><span class="log-tag">[${tag}]</span>${msg}`;
  log.prepend(line);
  while (log.children.length > 80) log.removeChild(log.lastChild);
}

// Update a standard device card (tv / hvac / bbq)
function setDevice(device, state, timestamp) {
  const val  = document.getElementById('val-'  + device);
  const sub  = document.getElementById('sub-'  + device);
  const dot  = document.getElementById('dot-'  + device);
  const card = document.getElementById('card-' + device);
  const seen = document.getElementById('seen-' + device);
  if (!val) return;

  const isOn = state && state !== 'OFF';

  val.textContent = state || '--';
  val.className   = 'state-value' + (isOn ? ' on' : '');
  dot.className   = 'dot' + (isOn ? ' on' : '');
  card.className  = 'device-card' + (isOn ? ' active' : '');

  if (device === 'hvac' && state) {
    sub.textContent = state === 'COOLING' ? 'mode: cooling'
                    : state === 'HEATING' ? 'mode: heating'
                    : 'mode: standby';
  } else if (device === 'humidifier') {
    sub.textContent = isOn ? 'active — low humidity' : 'inactive';
  } else {
    sub.textContent = isOn ? 'powered on' : 'powered off';
  }

  if (timestamp) {
    seen.textContent = 'last update: ' +
      new Date(timestamp).toLocaleTimeString('en-GB', { hour12: false });
  }
}

// Update the LED card — special handling for animations
function setLed(state, timestamp) {
  const val  = document.getElementById('val-led');
  const sub  = document.getElementById('sub-led');
  const dot  = document.getElementById('dot-led');
  const card = document.getElementById('card-led');
  const seen = document.getElementById('seen-led');
  if (!val) return;

  const isOn = state && state !== 'off' && state !== 'OFF';

  val.textContent = state || '--';
  val.className   = 'state-value' + (isOn ? ' led-on' : '');
  dot.className   = isOn && state === 'rainbow' ? 'dot led' : 'dot' + (isOn ? ' on' : '');
  card.className  = isOn ? 'device-card led-active' : 'device-card';

  const descriptions = {
    rainbow:   'animation: rainbow',
    breathing: 'animation: breathing',
    blink:     'animation: blink',
    alert:     'animation: alert',
    solid:     'animation: solid',
    chase:     'animation: chase',
    progress:  'animation: progress',
    off:       'strip off'
  };
  sub.textContent = descriptions[state] || (isOn ? 'active' : 'off');

  if (timestamp) {
    seen.textContent = 'last update: ' +
      new Date(timestamp).toLocaleTimeString('en-GB', { hour12: false });
  }
}

// Socket events
socket.on('connect',    () => addLog('state', 'SYS', 'socket connected'));
socket.on('disconnect', () => addLog('err',   'SYS', 'socket lost — reconnecting'));

socket.on('sensor-update', (readings) => {
  if (readings.gps.latitude != null) {
    document.getElementById('sen-lat').textContent  = Number(readings.gps.latitude).toFixed(4);
    document.getElementById('sen-lon').textContent  = Number(readings.gps.longitude).toFixed(4);
    document.getElementById('sen-dist').textContent = readings.gps.distance != null
      ? Number(readings.gps.distance).toFixed(2) : '--';
  }
  if (readings.temperature.value != null) {
    document.getElementById('sen-temp').textContent = Number(readings.temperature.value).toFixed(1) + ' °C';
  }
  if (readings.acceleration.magnitude != null) {
    document.getElementById('sen-mag').textContent = Number(readings.acceleration.magnitude).toFixed(2);
    document.getElementById('sen-xyz').textContent =
      `${Number(readings.acceleration.x).toFixed(1)} / ${Number(readings.acceleration.y).toFixed(1)} / ${Number(readings.acceleration.z).toFixed(1)}`;
  }
  if (readings.humidity.value != null) {
    document.getElementById('sen-hum').textContent = Number(readings.humidity.value).toFixed(1) + ' %';
  }
});

socket.on('device-state-update', (data) => {
  const ls = data.latestState;
  if (!ls) return;

  // Standard devices
  ['tv', 'hvac', 'bbq', 'humidifier'].forEach(d => {
    if (ls[d]) {
      setDevice(d, ls[d].state, ls[d].timestamp);
      addLog('state', d.toUpperCase(), `state → ${ls[d].state}`);
    }
  });

  // LED
  if (ls.led) {
    setLed(ls.led.state, ls.led.timestamp);
    addLog('led', 'LED', `animation → ${ls.led.state}`);
  }
});

// Device command (tv / hvac / bbq)
function sendCommand(device, command) {
  const stat = document.getElementById('cmdstat-' + device);
  if (stat) stat.textContent = 'sending...';

  fetch('/api/command', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device, command })
  })
  .then(r => r.json())
  .then(data => {
    if (stat) stat.textContent = data.success ? 'sent ' + ts() : 'failed';
    if (data.success) addLog('cmd', 'CMD', `${device.toUpperCase()} ← ${command}`);
    else              addLog('err', 'ERR', `${device.toUpperCase()} command rejected`);
  })
  .catch(() => {
    if (stat) stat.textContent = 'error';
    addLog('err', 'ERR', `${device.toUpperCase()} unreachable`);
  });
}

// LED command
function sendLedCommand(animation) {
  const stat = document.getElementById('cmdstat-led');
  if (stat) stat.textContent = 'sending...';

  fetch('/api/led-command', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ animation })
  })
  .then(r => r.json())
  .then(data => {
    if (stat) stat.textContent = data.success ? 'sent ' + ts() : 'failed';
    if (data.success) addLog('led', 'LED', `command sent → ${animation}`);
    else              addLog('err', 'ERR', `LED command rejected`);
  })
  .catch(() => {
    if (stat) stat.textContent = 'error';
    addLog('err', 'ERR', 'LED unreachable');
  });
}

// Clock
setInterval(() => {
  const el = document.getElementById('clock');
  if (el) el.textContent = ts();
}, 1000);

addLog('state', 'SYS', 'dashboard initialised');