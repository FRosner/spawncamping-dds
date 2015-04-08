function showTable(tableAndTypes) {

	var table = tableAndTypes.rows;
	var types = {};
	for (columnName in tableAndTypes.types) {
		types[columnName + " "] = tableAndTypes.types[columnName];
	}

    function generateParallelCoordinatesDiv(root, id) {
        var div = document.createElement('div');
        div.setAttribute("id", id);
        div.setAttribute("class", 'parcoords');
        div.style.height = window.innerHeight/5*2
        root.appendChild(div);
        var hideLabelButton = document.createElement('div');
        hideLabelButton.setAttribute("id", "hideLabelButton");
        document.getElementById("header").appendChild(hideLabelButton);
    }

    function generateGridDiv(root) {
    	var pager = document.createElement('div');
        pager.setAttribute("id", 'pager');
        root.appendChild(pager);
        var grid = document.createElement('div');
        grid.setAttribute("id", 'grid');
        grid.style.height = window.innerHeight/5*3 - 20
        root.appendChild(grid);
    }

    var numColumns = Object.keys(table[0]).length;
    var shouldDrawParcoords = numColumns > 1;

	// slickgrid needs each data element to have an id
	var ids = table.map(function(row, i) {
		return { id: i };
	});
	var data = ids.map(function(idObject, i) {
		var dataObject = { id: idObject.id }
		var tableRow = table[i];
		for (key in tableRow) {
			var keyWithIdReplaced = (key == "id") ? "_id" : key;
			dataObject[keyWithIdReplaced + " "] = tableRow[key];
		}
		return dataObject
	});

	var parcoords = {};
    if (shouldDrawParcoords) {
    	generateParallelCoordinatesDiv(document.getElementById("content"), "pcvis")
		var parcoords = d3.parcoords() ("#pcvis")
			.data(data)
			.dimensions(Object.keys(types))
			.types(types)
			.color("#1f77b4")
			.alpha(0.3)
			.margin({top:30, left:0, right:0, bottom:10})
			.width(window.innerWidth)
			.height(window.innerHeight/5*2)
			.mode("queue")
			.rate(60)
			.hideAxis(["id"])
			.render()
			.reorderable()
			.brushMode("1D-axes")
			.interactive();

		// Define a gradient for the color selector
		var gradient = d3.selectAll("svg").append("svg:defs")
			.append("svg:linearGradient")
			.attr("id", "gradient")
			.attr("x1", "0%")
			.attr("y1", "0%")
			.attr("x2", "100%")
			.attr("y2", "0%")
			.attr("spreadMethod", "pad");
		gradient.append("svg:stop")
			.attr("offset", "0%")
			.attr("stop-color", "#ffff00")
			.attr("stop-opacity", 1);
		gradient.append("svg:stop")
			.attr("offset", "100%")
			.attr("stop-color", "#ff5500")
			.attr("stop-opacity", 1);

		function changeColor(dimension) {
			var dimensions = parcoords.svg.selectAll(".dimension");
			dimensions.selectAll("circle")
				.attr("class", "colorSelector");

			if (!document.coloringEnabled || document.lastColoredDimension != dimension) {
				dimensions.filter(function(d) { return d == dimension; })
					.selectAll("circle")
					.attr("class", "colorSelector-selected");
				document.coloredDimension = dimension;
				var values = data.map(function(row) {
					return row[dimension];
				});
				var scale;
				if (types[dimension] == "string") {
					var uniqValues = _.uniq(values).reduce(function(uniqIndexes, value, index) {
						uniqIndexes[value] = index;
						return uniqIndexes;
					}, {});
					var domain = [0, Object.keys(uniqValues).length - 1];
					var chromaScale = chroma.scale('Set1').domain(domain);
					scale = function(v) {
						return chromaScale(uniqValues[v]);
					};
				} else {
					var domain = [Math.min.apply(null, values), Math.max.apply(null, values)];
					var chromaScale = chroma.scale(['orange', 'maroon']).domain(domain);
					scale = function(v) {
						return chromaScale(v);
					};
				}

				parcoords.color(function(d) {
					// color depending on selected dimension
					var value = d[dimension];
					return scale(value);
				}).render()
				document.coloringEnabled = true;
				document.lastColoredDimension = dimension;
			} else {
				parcoords.color("#1f77b4");
				document.coloringEnabled = false;
			}
		}

		// click circle to activate coloring
		parcoords.svg.selectAll(".dimension").selectAll(".axis")
			.append("circle")
			.attr("r", 4)
			.attr("class", "colorSelector")
			.attr("transform", "translate(0,-25)")
			.attr("text-anchor", "middle")
			.on("click", changeColor)
			.append("svg:title")
			.text("Color data based on this dimension");
		
		var labels = parcoords.svg.selectAll(".tick").selectAll("text");
		var button = document.getElementById("hideLabelButton");
		document.getElementById("hideLabelButton").onclick = function() {
			if (document.tickLabelsHidden) {
				labels.attr("visibility", "visible");
				button.setAttribute("class", "unhidden");
				button.setAttribute("title", "Hide Ticks Labels");
				document.tickLabelsHidden = false;	
			} else {
				labels.attr("visibility", "hidden");
				button.setAttribute("class", "hidden");
				button.setAttribute("title", "Show Ticks Labels");
				document.tickLabelsHidden = true;
			}
		};
		if (!document.tickLabelsHidden) {
			labels.attr("visibility", "visible");
			button.setAttribute("class", "unhidden");
			button.setAttribute("title", "Hide Ticks Labels");
		} else {
			labels.attr("visibility", "hidden");
			button.setAttribute("class", "hidden");
			button.setAttribute("title", "Show Ticks Labels");
		}
		
		document.coloringEnabled = false;
    } else {
    	var singleColumn = table.map(function(row) {
    		return row[Object.keys(row)[0]];
    	});
    	if (types[Object.keys(types)[0]] == "number") {
			var bins = d3.layout.histogram().bins(100)(singleColumn);
			bins = bins.map(function(bin) {
				return {
					start: bin.x,
					end: bin.x + bin.dx,
					y: bin.y
				};
			});
			showHistogram(bins,
			    window.innerWidth,
			    window.innerHeight/5*2,
			    {top: 15, right: 30, bottom: 30, left: 50});
    	} else {
    		generateChartDiv(document.getElementById("content"), "chart");
    		var singleColumnCounts = singleColumn.reduce(function(counts, value) {
				counts[value] = counts[value] ? counts[value] + 1 : 1;
				return counts;
			}, {});
			var singleColumnCountsForC3 = Object.keys(singleColumnCounts).map(function(key) {
				return { value:key, count:singleColumnCounts[key] };
			});
			var chart = {
			  data: {
				json: singleColumnCountsForC3,
				keys: {
					x: "value",
					value: ["count"]
				},
				type: "bar"
			  },
			  axis: {
				x: { type: "category" }
			  }
			};
			chart.size = {
				width: window.innerWidth,
				height: window.innerHeight/5*2
			};
			chart.padding = {
				right: 15,
				top: 10
			};
			c3.generate(chart);
    	}
    }

	generateGridDiv(document.getElementById("content"));

	// setting up grid
	var column_keys = d3.keys(data[0]);
	var columns = column_keys.map(function(key,i) {
		return {
			id: key,
			name: key,
			field: key,
			sortable: true
		}
	});

	var options = {
			enableCellNavigation: true,
			enableColumnReorder: false,
			multiColumnSort: false
	};

	var dataView = new Slick.Data.DataView();
	var grid = new Slick.Grid("#grid", dataView, columns, options);
	var pager = new Slick.Controls.Pager(dataView, grid, $("#pager"));

	// wire up model events to drive the grid
	dataView.onRowCountChanged.subscribe(function (e, args) {
		grid.updateRowCount();
		grid.render();
	});

	dataView.onRowsChanged.subscribe(function (e, args) {
		grid.invalidateRows(args.rows);
		grid.render();
	});

	// column sorting

	var sortcol = column_keys[0];
	var sortdir = 1;

	function comparer(a,b) {
		var x = a[sortcol], y = b[sortcol];
		return (x == y ? 0 : (x > y ? 1 : -1));
	}

	// click header to sort grid column
	grid.onSort.subscribe(function (e, args) {
		sortdir = args.sortAsc ? 1 : -1;
		sortcol = args.sortCol.field;

		if ($.browser.msie && $.broswer.version <= 8) {
			dataView.fastSort(sortcol, args.sortAsc);
		} else {
			dataView.sort(comparer, args.sortAsc);
		}
	});

	if (shouldDrawParcoords) {
		// highlight row in chart
		grid.onMouseEnter.subscribe(function(e,args) {
			var i = grid.getCellFromEvent(e).row;
			var d = parcoords.brushed() || data;
			parcoords.highlight([d[i]]);
		});
		grid.onMouseLeave.subscribe(function(e,args) {
			parcoords.unhighlight();
		});
		// update grid on brush
		parcoords.on("brush", function(d) {
			gridUpdate(d);
		});
	}

	// fill grid with data
	gridUpdate(data);

	function gridUpdate(data) {
		dataView.beginUpdate();
		dataView.setItems(data);
		dataView.endUpdate();
	};

}

function generateChartDiv(root, id) {
    var div = document.createElement('div');
    div.setAttribute("id", id);
    root.appendChild(div);
    return div;
}

function showSingleChart(chart) {
    generateChartDiv(document.getElementById("content"), "chart");
    chart.size = {
    	width: window.innerWidth,
    	height: window.innerHeight - 40 // -x to leave space for legends
    };
    chart.padding = {
    	right: 15
    };
    c3.generate(chart);
}

function showHistogram(bins, histWidth, histHeight, margin) {
    var chartDiv = generateChartDiv(document.getElementById("content"), "chart");
    chartDiv.className = "c3";

    var width = histWidth - margin.left - margin.right,
        height = histHeight - margin.top - margin.bottom;

    var svg = d3.select("#chart").append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.scale.linear()
        .range([0, width]);

    var y = d3.scale.linear()
        .range([height, 0]);

    x.domain([
        d3.min(bins.map(function(bin) { return bin.start; })),
        d3.max(bins.map(function(bin) { return bin.end; }))
    ]);

    bins = bins.map(function(bin) {
        bin.width = x(bin.end) - x(bin.start);
        bin.height = bin.y / (bin.end - bin.start);
        return bin;
    });

    y.domain([
        0,
        d3.max(bins.map(function(bin) { return bin.height; }))
    ]);

    bins = bins.map(function(bin) {
        bin.height = y(bin.height);
        return bin;
    });

    svg.selectAll(".bin")
        .data(bins)
        .enter().append("rect")
        .attr("fill", "steelblue")
        .attr("class", "bin")
        .attr("x", function(bin) { return x(bin.start); })
        .attr("width", function(bin) { return bin.width - 1; })
        .attr("y", function(bin) { return bin.height; })
        .attr("height", function(bin) { return height - bin.height; });

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

function showGraph(graph) {
	var width = window.innerWidth,
        height = window.innerHeight;

    var nodes = graph.vertices;
    var links = graph.edges;

    var svg = d3.select('#content').append('svg')
        .attr('width', width)
        .attr('height', height);

    var force = d3.layout.force()
        .size([width, height])
        .nodes(nodes)
        .links(links)
        .linkDistance(Math.min(width, height)/6.5);

    var links = svg.selectAll('.link')
        .data(links)
        .enter().append('line')
        .attr('class', 'link');

    var nodes = svg.selectAll('.node')
        .data(nodes)
        .enter()

    var circles = nodes.append('circle')
        .attr('class', 'node');

    var labels = nodes.append('text')
    	.text(function(n) { return n.label; })
		.attr('fill', 'black');

    force.on('tick', function() {
        circles.attr('r', 5)
            .attr('cx', function(n) { return n.x; })
            .attr('cy', function(n) { return n.y; });

        labels.attr('x', function(n) { return n.x+7; })
			.attr('y', function(n) { return n.y-4; })

        links.attr('x1', function(l) { return l.source.x; })
            .attr('y1', function(l) { return l.source.y; })
            .attr('x2', function(l) { return l.target.x; })
            .attr('y2', function(l) { return l.target.y; });
    });

    force.start();
}

function showScatter2D(points) {
	var chartDiv = generateChartDiv(document.getElementById("content"), "chart");
    chartDiv.className = "c3";

	var margin = {top: 20, right: 15, bottom: 60, left: 60}
      , width = window.innerWidth - margin.left - margin.right
      , height = window.innerHeight - margin.top - margin.bottom;
    
    var minX = d3.min(points, function(d) { return d.x; });
    var maxX = d3.max(points, function(d) { return d.x; });
    var dX = maxX - minX;
    var x = d3.scale.linear()
              .domain([minX - dX * 0.01, maxX + dX * 0.01])
              .range([ 0, width ]);
    
    var minY = d3.min(points, function(d) { return d.y; });
    var maxY = d3.max(points, function(d) { return d.y; });
    var dY = maxY - minY;
    var y = d3.scale.linear()
    	      .domain([minY - dY * 0.02, maxY + dY * 0.02])
    	      .range([ height, 0 ]);
 
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
        
    // draw the x axis
    var xAxis = d3.svg.axis()
	.scale(x)
	.orient('bottom');

    main.append('g')
	.attr('transform', 'translate(0,' + height + ')')
	.attr('class', 'x axis')
	.call(xAxis);

    // draw the y axis
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
      .enter().append("svg:circle")
          .attr("cx", function (d) { return x(d.x); } )
          .attr("cy", function (d) { return y(d.y); } )
          .attr("r", 3);
}
