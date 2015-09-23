define(function(require) {

  document.servablesCache = {};

  return {
    existsConfig: function(id) {
      return !(document.servablesCache[id] == null);
    },

    getConfig: function(id) {
      if (!document.servablesCache[id]) {
        // lazy initialization
        // TODO remove this lazy initialization
        document.servablesCache[id] = {};
      }
      return document.servablesCache[id];
    },

    setConfig: function(id, config) {
      document.servablesCache[id] = config;
    }
  };

});
