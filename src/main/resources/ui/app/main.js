define(function(require) {

  var $ = require("jquery"),
    Util = require("util"),
    Cache = require("Cache"),
    Drawer = require("draw");

  function checkForUpdate() {
    $.ajax({
      url: "/servables/latest" + ((document.lastServedId != null) ? "?current=" + document.lastServedId : ""),
      success: function(response) {
        if (response != "{}") {
          var servableAndId = JSON.parse(response);
          var servable = servableAndId.servable;
          document.lastServedId = servableAndId.id;
          document.isNewVisualization = true;
          Util.doAndRedoOnResizeOf(window, function() {
            var contentId = "content";
            var headerId = "header";
            if (document.lastServed) {
              document.lastServed.clear();
            }
            var previousContentDiv = document.getElementById(contentId);
            var contentParent = previousContentDiv.parentNode;
            Util.removeElementIfExists(previousContentDiv);
            Util.generateDiv(contentParent, contentId);
            document.lastServed = Drawer.drawServable(servable, headerId, contentId);
            document.isNewVisualization = false;
          });
        }
      }
    });
  }

  function toggleUpdating() {
    var lockButton = document.getElementById("lockButton");
    if (document.checkingForUpdate == true) {
      lockButton.className = "headerButton locked";
      lockButton.title = "Unlock Vizboard"
      document.checkingForUpdate = false;
      clearInterval(document.updater);
      document.updater = null;
    } else {
      lockButton.className = "headerButton unlocked"
      lockButton.title = "Lock Vizboard"
      document.updater = setInterval(checkForUpdate, 100);
      document.checkingForUpdate = true;
    }
  }

  return {
    start: function() {
      Cache.resetCache();
      $(document)
        .ready(toggleUpdating);
      $("#lockButton").click(toggleUpdating);
    }
  }
});
