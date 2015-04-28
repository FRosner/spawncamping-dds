// TODO used in main.js and Table.js => dependency for require.js
function C3Chart() {}

C3Chart.prototype = new Visualization();
C3Chart.prototype.constructor = Visualization;
C3Chart.prototype.parent = Visualization.prototype;

C3Chart.prototype._draw = function(chart) {
    this._chartDiv = generateDiv(document.getElementById("content"), "chart");
    chart.size = {
        width: this._width,
        height: this._height - 40 // -x to leave space for legends
    };
    chart.padding = this._margin;
    c3.generate(chart);
}

C3Chart.prototype.clearHeader = function() {
}
