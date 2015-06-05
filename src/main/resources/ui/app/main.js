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
  if (servable.type == "composite") {
    return new Composite()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 30,
        right: 0,
        bottom: 0,
        left: 0
      })
      .data(servable.content)
      .draw();
  } else if (servable.type == "chart") {
    return new C3Chart()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 5,
        right: 15,
        left: 60
      })
      .data(servable.content)
      .draw();
  } else if (servable.type == "table") {
    return new Table()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 30,
        right: 0,
        bottom: 0,
        left: 0
      })
      .data(servable.content)
      .draw();
  } else if (servable.type == "histogram") {
    return new Histogram()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 20,
        right: 60,
        bottom: 60,
        left: 60
      })
      .data(servable.content)
      .draw();
  } else if (servable.type == "graph") {
    return new Graph()
      .header(headerId)
      .content(contentId)
      .data(servable.content)
      .draw();
  } else if (servable.type == "points-2d") {
    return new Scatter2D()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 10,
        right: 15,
        bottom: 60,
        left: 60
      })
      .data(servable.content)
      .draw();
  } else if (servable.type == "matrix") {
    return new Matrix()
      .header(headerId)
      .content(contentId)
      .margin({
        top: 10,
        right: 15,
        bottom: 60,
        left: 60
      })
      .data(servable.content)
      .draw();
  } else {
    console.error("Unrecognized response: " + response);
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
            document.lastServed.clearHeader();
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
