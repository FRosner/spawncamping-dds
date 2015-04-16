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

	var zValues = matrix.map(function(v) {return v.z});
	var zDomain = [
		Math.min.apply(null, zValues),
		Math.max.apply(null, zValues)
	];
	var z = chroma.scale("YlOrRd").domain(zDomain);

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

    g.selectAll("matrix-rects")
      	.data(matrix)
		.enter().append("rect")
			.attr("class", "cell")
			.attr("x", function (p) {
				return x(colNames[p.x]) + 1;
          	})
          	.attr("y", function (p) {
          		return y(rowNames[p.y]);
          	})
			.attr("width", x.rangeBand() - 1)
			.attr("height", y.rangeBand() - 1)
			.attr("fill", function(value) {return z(value.z)})
			.attr("class", "matrix-cell")
			.append("svg:title")
			.text(function(value) {return value.z});
}

Matrix.prototype.clear = function() {
	removeElementIfExists(this._chartDiv);
}
