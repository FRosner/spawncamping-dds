

function showData(data) {

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
	data.forEach(function(d,i) { d.id = d.id || i; });


	var parcoords = d3.parcoords() ("#pcvis")
		.data(data)
		.width(window.innerWidth)
		.height(window.innerHeight/5*2)
		.mode("queue")
		.rate(60)
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

