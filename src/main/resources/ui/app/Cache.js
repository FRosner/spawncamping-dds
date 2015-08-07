define(function(require) {

  return {
    getCache: function(id) {
      if (!document.servablesCache[id]) {
        // lazy initialization
        document.servablesCache[id] = {};
      }
      return document.servablesCache[id];
    },

    resetCache: function() {
      document.servablesCache = {};
    }
  };

});
