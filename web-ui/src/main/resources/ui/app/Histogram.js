define(function(require) {

  var Visualization = require("Visualization");

  function Histogram() {}

  Histogram.prototype = new Visualization();
  Histogram.prototype.constructor = Visualization;
  Histogram.prototype.parent = Visualization.prototype;

  Histogram.prototype._draw = function(histogram) {
    var dds = require("dds");
    this._chartDiv = dds.histogram(histogram.title, histogram.bins, histogram.frequencies);
    this._content.appendChild(this._chartDiv);
  }

  Histogram.prototype._clear = function() {}

  return Histogram;

});
