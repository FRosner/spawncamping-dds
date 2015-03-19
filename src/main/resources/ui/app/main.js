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

function checkForUpdate() {
    $.ajax({
        url: "/chart/update",
        success: function(response) {
            if (response != "{}") {
                document.getElementById("content").innerHTML = "";
                var servable = JSON.parse(response);
                if (servable.type == "chart") {
                    generateSingleChart(servable.content)
                } else if (servable.type == "table") {
                    showData(servable.content)
                } else {
                    console.log("Unrecognized response: " + response);
                }
            }
        }
    });
}

function generateSingleChart(chart) {

    function generateChartDiv(root, id) {
        var div = document.createElement('div');
        div.setAttribute("id", id);
        root.appendChild(div);
    }

    generateChartDiv(document.getElementById("content"), "chart")
    var chart = c3.generate(chart);

}
