function Matrix() {}

Matrix.prototype = new Visualization();
Matrix.prototype.constructor = Visualization;
Matrix.prototype.parent = Visualization.prototype;

Matrix.prototype._draw = function(matrixAndNames) {
  var matrix = flatMap(matrixAndNames.entries, function(row, i) {
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

  this._chartDiv = generateDiv(this._content, "chart");
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
    return v.z
  });
  var zMin = (document.isNewVisualization) ? Math.min.apply(null, zValues) : document.lastServed._lowerBoundInput.value;
  var zMax = (document.isNewVisualization) ? Math.max.apply(null, zValues) : document.lastServed._upperBoundInput.value;
  var zDomain = [
    zMin,
    zMax
  ];
  var z = chroma.scale("YlOrRd")
    .domain(zDomain);

  var chart = d3.select("#chart")
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
      return z(value.z);
    })
    .attr("class", "matrix-cell")
  rects.append("svg:title")
    .text(function(value) {
      return value.z;
    });

  var lowerBoundInput = generateTextInput(this._header, "lowerBoundInput");
  lowerBoundInput.value = zMin;
  lowerBoundInput.onblur = function() {
    var customZDomain = [
      lowerBoundInput.value,
      upperBoundInput.value
    ];
    var customZ = chroma.scale("YlOrRd")
      .domain(customZDomain);
    rects.attr("fill", function(value) {
      return customZ(value.z);
    });
  };
  this._lowerBoundInput = lowerBoundInput;
  var upperBoundInput = generateTextInput(this._header, "upperBoundInput");
  upperBoundInput.value = zMax;
  upperBoundInput.onblur = function() {
    var customZDomain = [
      lowerBoundInput.value,
      upperBoundInput.value
    ];
    var customZ = chroma.scale("YlOrRd")
      .domain(customZDomain);
    rects.attr("fill", function(value) {
      return customZ(value.z);
    });
  };
  this._upperBoundInput = upperBoundInput;
}

Matrix.prototype.clearHeader = function() {
  removeElementIfExists(this._lowerBoundInput);
  removeElementIfExists(this._upperBoundInput);
}
