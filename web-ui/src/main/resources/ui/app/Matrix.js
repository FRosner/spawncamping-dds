define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util"),
    Cache = require("Cache");

  function Matrix(id) {
  }

  Matrix.prototype = new Visualization();
  Matrix.prototype.constructor = Visualization;
  Matrix.prototype.parent = Visualization.prototype;

// TODO implement a true multi-zero color scale that does not just take the number of zeroes but also the values
  Matrix.prototype._draw = function(matrixAndNames) {
    var dds = require("dds");
    this._chartDiv = dds.heatmap(matrixAndNames.title, matrixAndNames.content, matrixAndNames.rowNames,
                                 matrixAndNames.colNames, matrixAndNames.zColorZeroes);
    this._content.appendChild(this._chartDiv);
  }

  Matrix.prototype._clear = function() {
  }

  return Matrix;

});
