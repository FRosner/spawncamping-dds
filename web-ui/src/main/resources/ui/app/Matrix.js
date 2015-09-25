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
    var d3 = require("d3"),
      chroma = require("chroma");

    var vizId = this._content.id;
    var config = this.config;
    var matrix = Util.flatMap(matrixAndNames.content, function(row, i) {
      return row.map(function(entry, j) {
        return {
          x: j,
          y: i,
          z: entry
        };
      });
    });
    var rowNames = matrixAndNames.rowNames;
    var colNames = matrixAndNames.colNames;
    var zColorZeroes = matrixAndNames.zColorZeroes;
    if (zColorZeroes.length < 2) {
      console.error("zColorZeroes must have at least min and max but has only: " + zColorZeroes);
    }

    this._chartDiv = Util.generateDiv(this._content, "chart-" + vizId);
    this._chartDiv.className = "c3";

    var margin = this._margin;
    var width = this._width - margin.left - margin.right;
    var height = this._height - margin.top - margin.bottom;

    var x = d3.scale.ordinal()
      .domain(colNames)
      .rangeBands([0, width]);

    var y = d3.scale.ordinal()
      .domain(rowNames)
      .rangeBands([height, 0]);

    var zDomain = [
      matrixAndNames.zColorZeroes[0],
      matrixAndNames.zColorZeroes[zColorZeroes.length - 1]
    ];
    var zScaleString = "YlOrRd";
    if (zColorZeroes.length == 3) {
      zScaleString = "PRGn";
    } else if (zColorZeroes.length > 3) {
      console.warn("Currently only up to three colors are supported, but given: " + zColorZeroes);
    }
    var z = chroma.scale(zScaleString)
      .domain(zDomain);

    var chart = d3.select("#chart-" + vizId)
      .append('svg:svg')
      .attr('width', width + margin.right + margin.left)
      .attr('height', height + margin.top + margin.bottom)
      .attr('class', 'c3')

    var main = chart.append('g')
      .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
      .attr('width', width)
      .attr('height', height)
      .attr('class', 'main')

    var xAxis = d3.svg.axis()
      .scale(x)
      .orient('bottom');

    main.append('g')
      .attr('transform', 'translate(0,' + height + ')')
      .attr('class', 'x axis')
      .call(xAxis);

    var yAxis = d3.svg.axis()
      .scale(y)
      .orient('left');

    main.append('g')
      .attr('transform', 'translate(0,0)')
      .attr('class', 'y axis')
      .call(yAxis);

    var g = main.append("svg:g");

    var rects = g.selectAll("matrix-rects")
      .data(matrix)
      .enter()
      .append("rect")
      .attr("class", "cell")
      .attr("x", function(p) {
        return x(colNames[p.x]) + 1;
      })
      .attr("y", function(p) {
        return y(rowNames[p.y]);
      })
      .attr("width", x.rangeBand() - 1)
      .attr("height", y.rangeBand() - 1)
      .attr("fill", function(value) {
        return (value.z != null) ? z(value.z) : "#000000";
      })
      .attr("class", "matrix-cell")
    rects.append("svg:title")
      .text(function(value) {
        return value.z;
      });
  }

  Matrix.prototype._clear = function() {
  }

  return Matrix;

});
