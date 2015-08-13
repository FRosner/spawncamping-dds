define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util");

  function Matrix() {}

  Matrix.prototype = new Visualization();
  Matrix.prototype.constructor = Visualization;
  Matrix.prototype.parent = Visualization.prototype;

  Matrix.prototype._draw = function(matrixAndNames) {
    var d3 = require("d3"),
      chroma = require("chroma"),
      Cache = require("Cache");

    var vizId = this._content.id;
    var cache = Cache.getCache(vizId);
    var matrix = Util.flatMap(matrixAndNames.entries, function(row, i) {
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

    var zValues = matrix.map(function(v) {
      return v.z;
    });
    var zMin = (document.isNewVisualization) ? Math.min.apply(null, zValues) : cache.lowerBoundInput.value;
    var zMax = (document.isNewVisualization) ? Math.max.apply(null, zValues) : cache.upperBoundInput.value;
    var zDomain = [
      zMin,
      zMax
    ];
    this._defaultScale = "YlOrRd";
    var zScaleString;
    if (cache.heatMapScale) {
      zScaleString = cache.heatMapScale;
    } else {
      zScaleString = this._defaultScale;
      cache.heatMapScale = zScaleString;
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

    var boundArea = Util.generateSpan(this._header, "boundArea-" + vizId);
    boundArea.setAttribute("class", "boundArea");
    this._boundArea = boundArea;
    var zText1 = Util.generateSpan(this._boundArea, "");
    zText1.innerHTML = "z: "
    var lowerBoundInput = Util.generateTextInput(this._boundArea, "lowerBoundInput-" + vizId);
    lowerBoundInput.value = zMin;
    lowerBoundInput.setAttribute("class", "boundButton");
    lowerBoundInput.onblur = function() {
      var customZDomain = [
        lowerBoundInput.value,
        upperBoundInput.value
      ];
      var customZ = chroma.scale(cache.heatMapScale)
        .domain(customZDomain);
      rects.attr("fill", function(value) {
        return (value.z != null) ? customZ(value.z) : "#000000";
      });
    };
    this._lowerBoundInput = lowerBoundInput;
    cache.lowerBoundInput = lowerBoundInput;
    var zText2 = Util.generateSpan(this._boundArea, "");
    zText2.innerHTML = " - ";
    var upperBoundInput = Util.generateTextInput(this._boundArea, "upperBoundInput" + vizId);
    upperBoundInput.setAttribute("class", "boundButton");
    upperBoundInput.value = zMax;
    upperBoundInput.onblur = function() {
      var customZDomain = [
        lowerBoundInput.value,
        upperBoundInput.value
      ];
      var customZ = chroma.scale(cache.heatMapScale)
        .domain(customZDomain);
      rects.attr("fill", function(value) {
        return (value.z != null) ? customZ(value.z) : "#000000";
      });
    };
    this._upperBoundInput = upperBoundInput;
    cache.upperBoundInput = upperBoundInput;

    var redrawWithDifferentScale = function(scale) {
      return function() {
        if (cache.heatMapScale != scale) {
          cache.heatMapScale = scale;
          var zMin = cache.lowerBoundInput.value;
          var zMax = cache.upperBoundInput.value;
          var zDomain = [
            zMin,
            zMax
          ];
          var newZ = chroma.scale(scale)
            .domain(zDomain);
          rects.attr("fill", function(value) {
            return (value.z != null) ? newZ(value.z) : "#000000";
          });
        }
      }
    }

    var ylOrRdButton = document.createElement('div');
    ylOrRdButton.setAttribute("id", "ylOrRdButton-" + vizId);
    ylOrRdButton.setAttribute("class", "headerButton ylOrRdButton");
    ylOrRdButton.onclick = redrawWithDifferentScale("YlOrRd");
    this._header.appendChild(ylOrRdButton);
    this._ylOrRdButton = ylOrRdButton;

    var pRGnButton = document.createElement('div');
    pRGnButton.setAttribute("id", "pRGnButton-" + vizId);
    pRGnButton.setAttribute("class", "headerButton pRGnButton");
    pRGnButton.onclick = redrawWithDifferentScale("PRGn");
    this._header.appendChild(pRGnButton);
    this._pRGnButton = pRGnButton;
  }

  Matrix.prototype._clear = function() {
    Util.removeElementIfExists(this._lowerBoundInput);
    Util.removeElementIfExists(this._upperBoundInput);
    Util.removeElementIfExists(this._boundArea);
    Util.removeElementIfExists(this._ylOrRdButton);
    Util.removeElementIfExists(this._pRGnButton);
  }

  return Matrix;

});
