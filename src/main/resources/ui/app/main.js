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
                        generateSingleChart(servable.content)
                    } else if (servable.type == "table") {
                        showData(servable.content)
                    } else if (servable.type == "histogram") {
                        var bins = servable.content;
                        generateHistogram(bins);
                    } else {
                        console.log("Unrecognized response: " + response);
                    }
                });
            }
        }
    });
}

function generateChartDiv(root, id) {
        var div = document.createElement('div');
        div.setAttribute("id", id);
        root.appendChild(div);
        return div;
    }

function generateSingleChart(chart) {
    generateChartDiv(document.getElementById("content"), "chart")
    var chart = c3.generate(chart);
}

function generateHistogram(bins) {
    var chartDiv = generateChartDiv(document.getElementById("content"), "chart");
    chartDiv.className = "c3";

    var margin = {top: 30, right: 60, bottom: 60, left: 60},
        width = window.innerWidth - margin.left - margin.right,
        height = window.innerHeight - margin.top - margin.bottom;

    var svg = d3.select("#chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.scale.linear()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    x.domain([
        d3.min(bins.map(function(bin) { return bin.start; })),
        d3.max(bins.map(function(bin) { return bin.end; }))
    ]);

    bins = bins.map(function(bin) {
        bin.width = x(bin.end) - x(bin.start);
        bin.height = bin.y / (bin.end - bin.start);
        return bin;
    });

    y.domain([
        0,
        d3.max(bins.map(function(bin) { return bin.height; }))
    ]);

    bins = bins.map(function(bin) {
        bin.height = y(bin.height);
        return bin;
    });

    svg.selectAll(".bin")
        .data(bins)
        .enter().append("rect")
        .attr("fill", "steelblue")
        .attr("class", "bin")
        .attr("x", function(bin) { return x(bin.start); })
        .attr("width", function(bin) { return bin.width - 1; })
        .attr("y", function(bin) { return bin.height; })
        .attr("height", function(bin) { return height - bin.height; });

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.svg.axis()
        .scale(x)
        .orient("bottom"));

    svg.append("g")
        .attr("class", "y axis")
        .call(d3.svg.axis()
        .scale(y)
        .orient("left"));
}
