define(function(require) {

  var Visualization = require("Visualization");

  function Pie() {}

  Pie.prototype = new Visualization();
  Pie.prototype.constructor = Visualization;
  Pie.prototype.parent = Visualization.prototype;

  Pie.prototype._draw = function(servable) {
    var dds = require("dds");
    this._chartDiv = dds.pieChart(servable.title, servable.categoryCountPairs);
    this._content.appendChild(this._chartDiv);
  };

  Pie.prototype._clear = function() {};

  return Pie;

});
