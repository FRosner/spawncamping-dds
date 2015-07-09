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
    document.updater = setInterval("checkForUpdate()", 100);
    document.checkingForUpdate = true;
  }
}

$(document)
  .ready(toggleUpdating);

document.servablesCache = {};

function getCache(id) {
  if (!document.servablesCache[id]) {
    // lazy initialization
    document.servablesCache[id] = {};
  }
  return document.servablesCache[id];
}

function drawServable(servable, headerId, contentId) {
  var toDraw;
  if (servable.type == "composite") {
    toDraw = new Composite()
      .margin({
        top: 30,
        right: 0,
        bottom: 0,
        left: 0
      })
      .data(servable.content);
  } else if (servable.type == "empty") {
    toDraw = new Empty();
  } else if (servable.type == "chart") {
    toDraw = new C3Chart()
      .margin({
        top: 5,
        right: 15,
        left: 60
      })
      .data(servable.content);
  } else if (servable.type == "table") {
    toDraw = new Table()
      .margin({
        top: 30,
        right: 0,
        bottom: 0,
        left: 0
      })
      .data(servable.content);
  } else if (servable.type == "histogram") {
    toDraw = new Histogram()
      .margin({
        top: 20,
        right: 60,
        bottom: 60,
        left: 60
      })
      .data(servable.content);
  } else if (servable.type == "graph") {
    toDraw = new Graph()
      .data(servable.content);
  } else if (servable.type == "points-2d") {
    toDraw = new Scatter2D()
      .margin({
        top: 10,
        right: 15,
        bottom: 60,
        left: 60
      })
      .data(servable.content);
  } else if (servable.type == "matrix") {
    toDraw = new Matrix()
      .margin({
        top: 10,
        right: 15,
        bottom: 60,
        left: 60
      })
      .data(servable.content);
  } else {
    console.error("Unrecognized response: " + response);
  }
  if (toDraw != null) {
    toDraw = toDraw.header(headerId)
      .content(contentId)
      .title(servable.title)
      .draw();
    return toDraw;
  }
}

function checkForUpdate() {
  $.ajax({
    url: "/chart/update",
    success: function(response) {
      if (response != "{}") {
        var servable = JSON.parse(response);
        document.isNewVisualization = true;
        doAndRedoOnResize(function() {
          var contentId = "content";
          var headerId = "header";
          if (document.lastServed) {
            document.lastServed.clear();
          }
          document.getElementById(contentId)
            .innerHTML = "";
          document.lastServed = drawServable(servable, headerId, contentId);
          document.isNewVisualization = false;
        });
      }
    }
  });
}
