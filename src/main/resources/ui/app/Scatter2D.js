define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util"),
    Cache = require("Cache");

  function Scatter2D(id) {
    if (Cache.existsConfig(id)) {
      this.config = Cache.getConfig(id);
    } else {
      this.config = {
        jitterEnabled: false
      };
      Cache.setConfig(id, this.config);
    }
  }

  Scatter2D.prototype = new Visualization();
  Scatter2D.prototype.constructor = Visualization;
  Scatter2D.prototype.parent = Visualization.prototype;

  Scatter2D.prototype._draw = function(pointsWithTypes) {
    var d3 = require("d3");

    var scatterVis = this;
    var divId = "scatter-" + this._content.id;
    var config = this.config;

    function drawScatter() {
      var points = pointsWithTypes.points;
      var types = pointsWithTypes.types;

      scatterVis._chartDiv = Util.generateDiv(scatterVis._content, divId);
      scatterVis._chartDiv.className = "c3";

      var margin = scatterVis._margin;
      var width = scatterVis._width - margin.left - margin.right;
      var height = scatterVis._height - margin.top - margin.bottom;

      var x;
      if (types.x == "number") {
        var minX = d3.min(points, function(p) {
          return p.x;
        });
        var maxX = d3.max(points, function(p) {
          return p.x;
        });
        var dX = maxX - minX;
        x = d3.scale.linear()
          .domain([minX - dX * 0.01, maxX + dX * 0.01])
          .range([0, width]);
      } else {
        x = d3.scale.ordinal()
          .domain(_.uniq(points.map(function(p) {
            return p.x
          })))
          .rangeBands([0, width]);
      }

      var y;
      if (types.y == "number") {
        var minY = d3.min(points, function(p) {
          return p.y;
        });
        var maxY = d3.max(points, function(p) {
          return p.y;
        });
        var dY = maxY - minY;
        y = d3.scale.linear()
          .domain([minY - dY * 0.02, maxY + dY * 0.02])
          .range([height, 0]);
      } else {
        y = d3.scale.ordinal()
          .domain(_.uniq(points.map(function(p) {
            return p.y
          })))
          .rangeBands([height, 0]);
      }

      var chart = d3.select("#" + divId)
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

      g.selectAll("scatter-dots")
        .data(points)
        .enter()
        .append("svg:circle")
        .attr("cx", function(p) {
          if (types.x == "number") {
            return x(p.x)
          } else {
            var jitter = (config.jitterEnabled) ? (x.rangeBand() * (Math.random(1) - 0.5) * 0.4) : 0;
            return x(p.x) + (x.rangeBand() / 2) + jitter;
          }
        })
        .attr("cy", function(p) {
          if (types.y == "number") {
            return y(p.y)
          } else {
            var jitter = (config.jitterEnabled) ? (y.rangeBand() * (Math.random(1) - 0.5) * 0.4) : 0;
            return y(p.y) + (y.rangeBand() / 2) + jitter;
          }
        })
        .attr("r", 3);
    }

    var enableJitterButton = document.createElement('div');
    enableJitterButton.setAttribute("id", "enableJitterButton");
    this._header.appendChild(enableJitterButton);
    var contentId = this._content.id;
    enableJitterButton.onclick = function() {
      if (config.jitterEnabled) {
        config.jitterEnabled = false;
        enableJitterButton.setAttribute("class", "headerButton disabled");
        enableJitterButton.setAttribute("title", "Enable Jitter");
      } else {
        config.jitterEnabled = true;
        enableJitterButton.setAttribute("class", "headerButton enabled");
        enableJitterButton.setAttribute("title", "Disable Jitter");
      }
      document.getElementById(contentId)
        .innerHTML = "";
      drawScatter();
    };
    if (config.jitterEnabled) {
      enableJitterButton.setAttribute("class", "headerButton enabled");
      enableJitterButton.setAttribute("title", "Disable Jitter");
    } else {
      enableJitterButton.setAttribute("class", "headerButton disabled");
      enableJitterButton.setAttribute("title", "Enable Jitter");
    }
    this._enableJitterButton = enableJitterButton;
    drawScatter();
  }

  Scatter2D.prototype._clear = function() {
    Util.removeElementIfExists(this._enableJitterButton);
  }

  return Scatter2D;

});
