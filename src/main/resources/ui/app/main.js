define(function(require) {

  var $ = require("jquery"),
    Util = require("util"),
    Cache = require("Cache"),
    d3 = require("d3"),
    Drawer = require("draw");

  function updateServableSelector() {
    $.get('/servables', function(servablesString) {
      var servables = JSON.parse(servablesString).concat([{id: "latest"}]).reverse();
      // TODO factor this out to Utils
      function pad(num) {
        return ("0" + num).substr(-2);
      };
      d3.select("#servableSelector")
        .selectAll("option")
        .data(servables, function(servable) {return (servable) ? servable.id : d3.select(this).attr("value");})
        .enter()
        .append("option")
        .text(function(servable) {
          if (servable.id == "latest") {
            return "Latest";
          } else {
            var date = new Date(servable.time);
            var dateString = pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds());
            return servable.id + " - " + servable.type + " (" + dateString + ")";
          }
        }).attr("value", function(servable) {
          return servable.id;
        });
    });
  }

  function handleServableResponse(response) {
    if (response != "{}") {
      updateServableSelector();
      var servableAndId = JSON.parse(response);
      var servable = servableAndId.servable;
      // TODO put the ID in also in the back-end
      servable.id = servableAndId.id;
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

  function checkForUpdate() {
    $.ajax({
      url: "/servables/latest" + ((document.lastServedId != null) ? "?current=" + document.lastServedId : ""),
      success: handleServableResponse
    });
  }

  function toggleUpdating() {
    var lockButton = document.getElementById("lockButton");
    if (document.checkingForUpdate == true) {
      lockButton.className = "headerButton locked";
      lockButton.title = "Unlock Vizboard";
      document.checkingForUpdate = false;
      clearInterval(document.updater);
      document.updater = null;
    } else {
      lockButton.className = "headerButton unlocked";
      lockButton.title = "Lock Vizboard";
      document.getElementById("servableSelector").value = "latest";
      document.updater = setInterval(checkForUpdate, 100);
      document.checkingForUpdate = true;
    }
  }

  return {
    start: function() {
      $(document)
        .ready(toggleUpdating);
      $(document).ready(function() {
        $("#lockButton").click(toggleUpdating);
        updateServableSelector();
        $("#servableSelector").change(function(x) {
          var selectedServable = document.getElementById("servableSelector").value;
          if (selectedServable == "latest" && document.checkingForUpdate == false) {
            toggleUpdating();
          } else {
            if (document.checkingForUpdate == true) {
              toggleUpdating();
            }
            $.ajax({
              url: "/servables/" + selectedServable,
              success: handleServableResponse
            });
          }
        });
      });
    }
  }

});
