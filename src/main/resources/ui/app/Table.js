function Table() {}

Table.prototype = new Visualization();
Table.prototype.constructor = Visualization;
Table.prototype.parent = Visualization.prototype;

Table.prototype._draw = function(tableAndTypes) {
  	var table = tableAndTypes.rows;
  	var types = {};
  	for (columnName in tableAndTypes.types) {
  	    types[columnName + " "] = tableAndTypes.types[columnName];
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
        this._parcoordsDiv = document.createElement('div');
        this._parcoordsDiv.setAttribute("id", "pcvis");
        this._parcoordsDiv.setAttribute("class", 'parcoords');
        this._parcoordsDiv.style.height = this._height/5*2
        this._content.appendChild(this._parcoordsDiv);
        this._hideLabelButton = document.createElement('div');
        this._hideLabelButton.setAttribute("id", "hideLabelButton");
        this._header.appendChild(this._hideLabelButton);
		    var parcoords = d3.parcoords() ("#pcvis")
			      .data(data)
      			.dimensions(Object.keys(types))
      			.types(types)
      			.color("#1f77b4")
      			.alpha(0.3)
      			.margin({top:this._margin.top, left:this._margin.left, right:this._margin.right, bottom:10})
      			.width(this._width)
      			.height(this._height/5*2)
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
      			var hist = new Histogram()
      			    .header("header")
      			    .content("content")
      			    .margin({top: 15, right: 30, bottom: 30, left: 50})
                .width(window.innerWidth)
                .height(window.innerHeight/5*2)
      			    .data(bins)
                .draw();
      	} else {
        		generateDiv(this._content, "chart");
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
      				  width: this._width,
      				  height: this._height/5*2
      			};
      			chart.padding = {
      				  right: 15,
      				  top: 10
      			};
      			c3.generate(chart);
      	}
    }

    var pager = document.createElement('div');
    pager.setAttribute("id", 'pager');
    this._content.appendChild(pager);
    var grid = document.createElement('div');
    grid.setAttribute("id", 'grid');
    grid.style.height = this._height/5*3 - 20
    this._content.appendChild(grid);

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

Table.prototype.clear = function() {
  // TODO only remove elements that have been added by the viz rather than clearing the whole content element
	this._content.innerHTML = "";
	removeElementIfExists(this._hideLabelButton);
}
