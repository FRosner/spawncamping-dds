function showTable(table) {

    function generatePCVis(root, id) {
        var div = document.createElement('div');
        div.setAttribute("id", id);
        div.setAttribute("class", 'parcoords');
        div.style.height = window.innerHeight/5*2
        root.appendChild(div);
        var pager = document.createElement('div');
        pager.setAttribute("id", 'pager');
        root.appendChild(pager);
        var grid = document.createElement('div');
        grid.setAttribute("id", 'grid');
        grid.style.height = window.innerHeight/5*3 - 20
        root.appendChild(grid);
    }

    generatePCVis(document.getElementById("content"), "pcvis")

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

	var parcoords = d3.parcoords() ("#pcvis")
		.data(data)
		.width(window.innerWidth)
		.height(window.innerHeight/5*2)
		.mode("queue")
		.rate(60)
		.hideAxis(["id"])
		.render()
		.reorderable()
		.brushMode("1D-axes");

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

	// highlight row in chart
	grid.onMouseEnter.subscribe(function(e,args) {
		var i = grid.getCellFromEvent(e).row;
		var d = parcoords.brushed() || data;
		parcoords.highlight([d[i]]);
	});
	grid.onMouseLeave.subscribe(function(e,args) {
		parcoords.unhighlight();
	});

	// fill grid with data
	gridUpdate(data);

	// update grid on brush
	parcoords.on("brush", function(d) {
		gridUpdate(d);
	});

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
    	right: 15,
    };
    c3.generate(chart);
}

function showHistogram(bins) {
    var chartDiv = generateChartDiv(document.getElementById("content"), "chart");
    chartDiv.className = "c3";

    var margin = {top: 30, right: 60, bottom: 60, left: 60},
        width = window.innerWidth - margin.left - margin.right,
        height = window.innerHeight - margin.top - margin.bottom;

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
        .linkDistance(Math.min(width, height)/7);

    var link = svg.selectAll('.link')
        .data(links)
        .enter().append('line')
        .attr('class', 'link');

    var node = svg.selectAll('.node')
        .data(nodes)
        .enter().append('circle')
        .attr('class', 'node');

    force.on('end', function() {
        node.attr('r', 5)
            .attr('cx', function(n) { return n.x; })
            .attr('cy', function(n) { return n.y; });

        link.attr('x1', function(l) { return l.source.x; })
            .attr('y1', function(l) { return l.source.y; })
            .attr('x2', function(l) { return l.target.x; })
            .attr('y2', function(l) { return l.target.y; });
    });

    force.start();
}
