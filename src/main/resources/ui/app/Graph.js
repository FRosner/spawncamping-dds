// also need to adjust Table.js in addition to main.js
// require.js needed to manage dependencies between modules
function Graph() {}

Graph.prototype = new Visualization();
Graph.prototype.constructor = Visualization;
Graph.prototype.parent = Visualization.prototype;

Graph.prototype._draw = function(graph) {
  var divId = "graph-" + this._content.id;
  var cache = getCache(divId);

  var width = this._width,
    height = this._height;

  var nodes = graph.vertices;
  var links = graph.edges;

  this._graphDiv = generateDiv(this._content, divId);

  var svg = d3.select("#" + divId)
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  // arrow heads
  svg.append("svg:marker")
    .attr("id", "triangle")
    .attr("viewBox", "0 0 10 10")
    .attr("refX", 16)
    .attr("refY", 5)
    .attr("markerUnits", "strokeWidth")
    .attr("markerWidth", 7)
    .attr("markerHeight", 7)
    .attr("orient", "auto")
    .attr("class", "arrowHead")
    .append("svg:path")
    .attr("d", "M 0 0 L 10 5 L 0 10 z");

  var force = d3.layout.force()
    .size([width, height])
    .nodes(nodes)
    .links(links)
    .linkDistance(
      Math.min(width, height) / 6.5)
    .charge(-500);

  var links = svg.selectAll('.link')
    .data(links)
    .enter();

  var linkLines = links.append("line")
    .attr("class", "link");

  var linkLabels = links.append('text')
    .text(function(l) {
      return l.label;
    })
    .attr('fill', 'black')
    .attr("class", "edgeLabel")
    .attr("text-anchor", "middle");

  var nodes = svg.selectAll('.node')
    .data(nodes)
    .enter();

  var circles = nodes.append('circle')
    .attr('class', 'node')
    .call(force.drag);

  var nodeLabels = nodes.append('text')
    .text(function(n) {
      return n.label;
    })
    .attr('fill', 'black')
    .attr("class", "nodeLabel");

  force.on('tick', function() {
    circles.attr('r', 5)
      .attr('cx', function(n) {
        return n.x;
      })
      .attr('cy', function(n) {
        return n.y;
      });

    nodeLabels.attr('x', function(n) {
        return n.x + 7;
      })
      .attr('y', function(n) {
        return n.y - 4;
      });

    linkLabels.attr('x', function(l) {
        if (l.target.x > l.source.x) {
          return l.source.x + (l.target.x - l.source.x) / 2;
        } else {
          return l.target.x + (l.source.x - l.target.x) / 2;
        }
      })
      .attr('y', function(l) {
        if (l.target.y > l.source.y) {
          return l.source.y + (l.target.y - l.source.y) / 2;
        } else {
          return l.target.y + (l.source.y - l.target.y) / 2;
        }
      });

    linkLines.attr('x1', function(l) {
        return l.source.x;
      })
      .attr('y1', function(l) {
        return l.source.y;
      })
      .attr('x2', function(l) {
        return l.target.x;
      })
      .attr('y2', function(l) {
        return l.target.y;
      });
  });

  force.start();

  var nodeButton = document.createElement('div');
  nodeButton.onclick = function() {
    if (cache.drawNodeLabels === false) {
      nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton visible");
      nodeButton.setAttribute("title", "Hide node labels");
      cache.drawNodeLabels = true;
      svg.selectAll(".nodeLabel")
        .style("visibility", "visible");
    } else {
      nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton hidden");
      nodeButton.setAttribute("title", "Draw node labels");
      cache.drawNodeLabels = false;
      svg.selectAll(".nodeLabel")
        .style("visibility", "hidden");
    }
  }
  if (cache.drawNodeLabels === false) {
    nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton hidden");
    nodeButton.setAttribute("title", "Draw node labels");
    svg.selectAll(".nodeLabel")
      .style("visibility", "hidden");
  } else {
    nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton visible");
    nodeButton.setAttribute("title", "Hide node labels");
    svg.selectAll(".nodeLabel")
      .style("visibility", "visible");
    cache.drawNodeLabels = true;
  }
  this._header.appendChild(nodeButton);
  this._triggerNodeLabelsButton = nodeButton;

  var edgeButton = document.createElement('div');
  edgeButton.onclick = function() {
    if (cache.drawEdgeLabels === false) {
      edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton visible");
      edgeButton.setAttribute("title", "Hide edge labels");
      cache.drawEdgeLabels = true;
      svg.selectAll(".edgeLabel")
        .style("visibility", "visible");
    } else {
      edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton hidden");
      edgeButton.setAttribute("title", "Draw edge labels");
      cache.drawEdgeLabels = false;
      svg.selectAll(".edgeLabel")
        .style("visibility", "hidden");
    }
  }
  if (cache.drawEdgeLabels === false) {
    edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton hidden");
    edgeButton.setAttribute("title", "Draw edge labels");
    svg.selectAll(".edgeLabel")
      .style("visibility", "hidden");
  } else {
    edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton visible");
    edgeButton.setAttribute("title", "Hide edge labels");
    svg.selectAll(".edgeLabel")
      .style("visibility", "visible");
    cache.drawEdgeLabels = true;
  }
  this._header.appendChild(edgeButton);
  this._triggerEdgeLabelsButton = edgeButton;

  var directionButton = document.createElement('div');
  directionButton.setAttribute("id", "triggerDirectionsButton");
  directionButton.onclick = function() {
    if (cache.drawDirections === false) {
      directionButton.setAttribute("class", "triggerDirectionsButton headerButton visible");
      directionButton.setAttribute("title", "Draw undirected edges");
      cache.drawDirections = true;
      svg.selectAll(".link")
        .attr("marker-end", "url(#triangle)");
    } else {
      directionButton.setAttribute("class", "triggerDirectionsButton headerButton hidden");
      directionButton.setAttribute("title", "Draw directed edges");
      cache.drawDirections = false;
      svg.selectAll(".link")
        .attr("marker-end", "");
    }
  }
  if (cache.drawDirections === false) {
    directionButton.setAttribute("class", "triggerDirectionsButton headerButton hidden");
    directionButton.setAttribute("title", "Draw directed edges");
    svg.selectAll(".link")
      .attr("marker-end", "");
  } else {
    directionButton.setAttribute("class", "triggerDirectionsButton headerButton visible");
    directionButton.setAttribute("title", "Draw undirected edges");
    svg.selectAll(".link")
      .attr("marker-end", "url(#triangle)");
    cache.drawDirections = true;
  }
  this._header.appendChild(directionButton);
  this._triggerDirectionsButton = directionButton;

}

Graph.prototype._clear = function() {
  removeElementIfExists(this._triggerNodeLabelsButton);
  removeElementIfExists(this._triggerEdgeLabelsButton);
  removeElementIfExists(this._triggerDirectionsButton);
}
