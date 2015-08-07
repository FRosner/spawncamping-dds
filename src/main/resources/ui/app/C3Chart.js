define(function(require) {

  var Visualization = require("Visualization");

  function C3Chart() {}

  C3Chart.prototype = new Visualization();
  C3Chart.prototype.constructor = Visualization;
  C3Chart.prototype.parent = Visualization.prototype;

  C3Chart.prototype._draw = function(chart) {
    var Util = require("util"),
      c3 = require("c3");

    this._chartDiv = Util.generateDiv(this._content, "chart-" + this._content.id);
    chart.bindto = "#" + this._chartDiv.id;
    var marginForLegend = 40;
    chart.size = {
      width: this._width,
      height: this._height - marginForLegend
    };
    chart.padding = this._margin;
    c3.generate(chart);
  }

  C3Chart.prototype._clear = function() {}

  return C3Chart;

});
