function toggleUpdating() {
    if (document.checkingForUpdate == true) {
        document.getElementById("lockButton").className = "locked";
        document.checkingForUpdate = false;
        clearInterval(document.updater);
        document.updater = null;
    } else {
        document.getElementById("lockButton").className = "unlocked";
        document.updater = setInterval("checkForUpdate()", 100);
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
                    generateTable(servable.content)
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

    generateChartDiv(document.body, "chart")
    var chart = c3.generate(chart);

}

function generateTable(stats) {

    function generateTableSkeleton(root, id) {
        var table = document.createElement('table');
        table.setAttribute("id", id);
        var tableHead = document.createElement('thead');
        var tableBody = document.createElement('tbody');
        table.appendChild(tableHead);
        table.appendChild(tableBody);
        root.appendChild(table);
    }

    function generatePCVis(root, id) {
        var div = document.createElement('div');
        div.setAttribute("id", id);
        div.setAttribute("class", 'parcoords');
        div.setAttribute("style", 'height:350px');
        root.appendChild(div);
    }

    generatePCVis(document.getElementById("content"), "pcvis")

    var parcoords = d3.parcoords()("#pcvis")
        .data(stats)
        .alpha(0.5)
        .composite("darker")
        .reorderable()
        .interactive()
        .render()
        .brushMode("1D-axes");

    generateTableSkeleton(document.getElementById("content"), "table")

    var tableHead = d3.select("thead").selectAll("th")
        .data(d3.keys(stats[0]))
        .enter().append("th").text(function(key){ return key });
    var tr = d3.select("tbody").selectAll("tr")
        .data(stats).enter().append("tr");

    var td = tr.selectAll("td")
      .data(function(rows){ return d3.values(rows) })
      .enter().append("td")
      .text(function(value){ return value });
}
