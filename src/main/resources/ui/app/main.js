function toggleUpdating() {
    var lockButton = document.getElementById("lockButton");
    if (document.checkingForUpdate == true) {
        lockButton.className = "locked";
        lockButton.title = "Unlock Vizboard"
        document.checkingForUpdate = false;
        clearInterval(document.updater);
        document.updater = null;
    } else {
        lockButton.className = "unlocked"
        lockButton.title = "Lock Vizboard"
        document.updater = setInterval("checkForUpdate()",100);
        document.checkingForUpdate = true;
    }
}

$(document).ready(toggleUpdating);

function clearContent() {
    document.getElementById("content").innerHTML = "";
}

function doAndRedoOnResize(f) {
    f();
    window.onresize = f;
}

function checkForUpdate() {
    $.ajax({
        url: "/chart/update",
        success: function(response) {
            if (response != "{}") {
                var servable = JSON.parse(response);
                doAndRedoOnResize(function() {
                    clearContent();
                    if (servable.type == "chart") {
                        showSingleChart(servable.content)
                    } else if (servable.type == "table") {
                        showTable(servable.content)
                    } else if (servable.type == "histogram") {
                        showHistogram(servable.content,
                            window.innerWidth,
                            window.innerHeight,
                            {top: 30, right: 60, bottom: 60, left: 60});
                    } else if (servable.type == "graph") {
                        showGraph(servable.content);
                    } else {
                        console.log("Unrecognized response: " + response);
                    }
                });
            }
        }
    });
}
