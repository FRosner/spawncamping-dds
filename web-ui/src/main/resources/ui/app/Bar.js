define(function(require) {

  var Visualization = require("Visualization");

  function Bar() {};

  Bar.prototype = new Visualization();
  Bar.prototype.constructor = Visualization;
  Bar.prototype.parent = Visualization.prototype;

  Bar.prototype._draw = function(servable) {
    var dds = require("dds");
    this._chartDiv = dds.barchart(servable.title, servable.xDomain, servable.heights, servable.series);
    this._content.appendChild(this._chartDiv);
  };

  Bar.prototype._clear = function() {};

  return Bar;

});
