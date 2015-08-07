define(function(require) {

  var Visualization = require("Visualization");

  function Histogram() {}

  Histogram.prototype = new Visualization();
  Histogram.prototype.constructor = Visualization;
  Histogram.prototype.parent = Visualization.prototype;

  Histogram.prototype._draw = function(bins) {
    var Util = require("util"),
      d3 = require("d3");

    var divId = "histogram-" + this._content.id;

    this._chartDiv = Util.generateDiv(this._content, divId);
    this._chartDiv.className = "c3";

    var margin = this._margin;
    var width = this._width - margin.left - margin.right,
      height = this._height - margin.top - margin.bottom;

    var svg = d3.select("#" + divId)
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.scale.linear()
      .range([0, width]);

    var y = d3.scale.linear()
      .range([height, 0]);

    x.domain([
      d3.min(bins.map(function(bin) {
        return bin.start;
      })),
      d3.max(bins.map(function(bin) {
        return bin.end;
      }))
    ]);

    bins = bins.map(function(bin) {
      bin.width = x(bin.end) - x(bin.start);
      bin.height = bin.y / (bin.end - bin.start);
      return bin;
    });

    y.domain([
      0,
      d3.max(bins.map(function(bin) {
        return bin.height;
      }))
    ]);

    bins = bins.map(function(bin) {
      bin.height = y(bin.height);
      return bin;
    });

    svg.selectAll(".bin")
      .data(bins)
      .enter()
      .append("rect")
      .attr("fill", "steelblue")
      .attr("class", "bin")
      .attr("x", function(bin) {
        return x(bin.start);
      })
      .attr("width", function(bin) {
        return bin.width - 1;
      })
      .attr("y", function(bin) {
        return bin.height;
      })
      .attr("height", function(bin) {
        return height - bin.height;
      });

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

  Histogram.prototype._clear = function() {}

  return Histogram;

});
