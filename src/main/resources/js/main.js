$(document).ready(function(){
    setInterval("checkForUpdate()",100);
});

function checkForUpdate() {

    $.ajax({
        url: "/chart/update",
        success: function(response) {
            if (response != "{}") {
                document.body.innerHTML = "";
                var servable = JSON.parse(response);
                if (servable.type == "chart") {
                    generateSingleChart(servable.content)
                } else if (servable.type == "stats") {
                    generateStatsTable(servable.content)
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

function generateStatsTable(stats) {

    function generateTableSkeleton(root, id) {
        var table = document.createElement('table');
        table.setAttribute("id", id);
        var tableHead = document.createElement('thead');
        var tableBody = document.createElement('tbody');
        table.appendChild(tableHead);
        table.appendChild(tableBody);
        root.appendChild(table);
    }

    generateTableSkeleton(document.body, "statsTable")

    var tableHead = d3.select("thead").selectAll("th")
        .data(d3.keys(stats[0]))
        .enter().append("th").text(function(key){ console.log("key: " + key); return key });
    var tr = d3.select("tbody").selectAll("tr")
        .data(stats).enter().append("tr");

    var td = tr.selectAll("td")
      .data(function(rows){ console.log("rows: " + JSON.stringify(rows)); return d3.values(rows) })
      .enter().append("td")
      .text(function(value){ console.log("value: " + value); return value });

}

