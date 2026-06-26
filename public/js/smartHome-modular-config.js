
const SMART_HOME_MODULAR_CONFIG = {
 
  location: {
    latitudeId: 'latitude-modular',
    longitudeId: 'longitude-modular',
    distanceValueId: 'distance-value-modular',
    temperatureValueId: 'temperature-value-modular'
  },

  // Device definitions
  devices: {
    hvac: {
      name: 'HVAC',
      statusId: 'hvac-status-modular',
      textId: 'hvac-text-modular',
      modeContainerId: 'hvac-mode-modular',
      modeTextId: 'hvac-mode-text-modular',
      hasMode: true,
      onClass: 'on',
      offClass: 'off',
      onText: 'ON',
      offText: 'OFF',
      update: function(element, textElement, device) {
        if (device.isPowerOn) {
          element.classList.add(this.onClass);
          element.classList.remove(this.offClass);
          textElement.textContent = this.onText;

          const modeContainer = document.getElementById(this.modeContainerId);
          const modeText = document.getElementById(this.modeTextId);
          modeContainer.style.display = 'flex';

          if (device.mode === 'HEATING') {
            modeText.textContent = 'Heating';
          } else if (device.mode === 'COOLING') {
            modeText.textContent = 'Cooling';
          }
        } else {
          element.classList.remove(this.onClass);
          element.classList.add(this.offClass);
          textElement.textContent = this.offText;

          const modeContainer = document.getElementById(this.modeContainerId);
          modeContainer.style.display = 'none';
        }
      }
    },

    tv: {
      name: 'Smart TV',
      statusId: 'tv-status-modular',
      textId: 'tv-text-modular',
      hasMode: false,
      onClass: 'on',
      offClass: 'off',
      onText: 'ON',
      offText: 'OFF',
      update: function(element, textElement, device) {
        if (device.isPowerOn) {
          element.classList.add(this.onClass);
          element.classList.remove(this.offClass);
          textElement.textContent = this.onText;
        } else {
          element.classList.remove(this.onClass);
          element.classList.add(this.offClass);
          textElement.textContent = this.offText;
        }
      }
    },

    barbecue: {
      name: 'Barbecue',
      statusId: 'barbecue-status-modular',
      textId: 'barbecue-text-modular',
      hasMode: false,
      onClass: 'on',
      offClass: 'off',
      onText: 'ON',
      offText: 'OFF',
      update: function(element, textElement, device) {
        if (device.isPowerOn) {
          element.classList.add(this.onClass);
          element.classList.remove(this.offClass);
          textElement.textContent = this.onText;
        } else {
          element.classList.remove(this.onClass);
          element.classList.add(this.offClass);
          textElement.textContent = this.offText;
        }
      }
    },

    lights: {
      name: 'Smart Lights',
      statusId: 'lights-status-modular',
      textId: 'lights-text-modular',
      hasMode: false,
      onClass: 'on',
      offClass: 'off',
      onText: 'ON',
      offText: 'OFF',
      update: function(element, textElement, device) {
        if (device.isPowerOn) {
          element.classList.add(this.onClass);
          element.classList.remove(this.offClass);
          textElement.textContent = this.onText;
        } else {
          element.classList.remove(this.onClass);
          element.classList.add(this.offClass);
          textElement.textContent = this.offText;
        }
      }
    }
  }
};
