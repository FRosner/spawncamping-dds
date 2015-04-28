// also need to adjust Table.js in addition to main.js
// require.js needed to manage dependencies between modules
function Graph() {}

Graph.prototype = new Visualization();
Graph.prototype.constructor = Visualization;
Graph.prototype.parent = Visualization.prototype;

Graph.prototype._draw = function(graph) {
    var width = this._width,
        height = this._height;

    var nodes = graph.vertices;
    var links = graph.edges;

    this._graphDiv = generateDiv(this._content, "graph");

    var svg = d3.select('#graph').append('svg')
        .attr('width', width)
        .attr('height', height);

    var force = d3.layout.force()
        .size([width, height])
        .nodes(nodes)
        .links(links)
        .linkDistance(Math.min(width, height)/6.5);

    var links = svg.selectAll('.link')
        .data(links)
        .enter();

    var linkLines = links.append('line')
        .attr('class', 'link');

    var linkLabels = links.append('text')
      .text(function(l) { return l.label; })
    .attr('fill', 'black')
    .attr("class", "edgeLabel")
    .attr("text-anchor", "middle");

    var nodes = svg.selectAll('.node')
        .data(nodes)
        .enter();

    var circles = nodes.append('circle')
        .attr('class', 'node');

    var nodeLabels = nodes.append('text')
      .text(function(n) { return n.label; })
    .attr('fill', 'black')
    .attr("class", "nodeLabel");

    force.on('tick', function() {
        circles.attr('r', 5)
            .attr('cx', function(n) { return n.x; })
            .attr('cy', function(n) { return n.y; });

        nodeLabels.attr('x', function(n) { return n.x+7; })
            .attr('y', function(n) { return n.y-4; });

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

        linkLines.attr('x1', function(l) { return l.source.x; })
            .attr('y1', function(l) { return l.source.y; })
            .attr('x2', function(l) { return l.target.x; })
            .attr('y2', function(l) { return l.target.y; });
    });

    force.start();

    var nodeButton = document.createElement('div');
    nodeButton.setAttribute("id", "triggerNodeLabelsButton");
    nodeButton.onclick = function() {
      if (document.drawNodeLabels === false) {
        nodeButton.setAttribute("class", "visible");
        nodeButton.setAttribute("title", "Hide node labels");
        document.drawNodeLabels = true;
        d3.selectAll(".nodeLabel").style("visibility", "visible");
      } else {
        nodeButton.setAttribute("class", "hidden");
        nodeButton.setAttribute("title", "Draw node labels");
        document.drawNodeLabels = false;
        d3.selectAll(".nodeLabel").style("visibility", "hidden");
      }
    }
    if (document.drawNodeLabels === false) {
      nodeButton.setAttribute("class", "hidden");
      nodeButton.setAttribute("title", "Draw node labels");
      d3.selectAll(".nodeLabel").style("visibility", "hidden");
    } else {
      nodeButton.setAttribute("class", "visible");
      nodeButton.setAttribute("title", "Hide node labels");
      d3.selectAll(".nodeLabel").style("visibility", "visible");
      document.drawNodeLabels = true;
    }
    this._header.appendChild(nodeButton);
    this._triggerNodeLabelsButton = nodeButton;

    var edgeButton = document.createElement('div');
    edgeButton.setAttribute("id", "triggerEdgeLabelsButton");
    edgeButton.onclick = function() {
      if (document.drawEdgeLabels === false) {
        edgeButton.setAttribute("class", "visible");
        edgeButton.setAttribute("title", "Hide edge labels");
        document.drawEdgeLabels = true;
        d3.selectAll(".edgeLabel").style("visibility", "visible");
      } else {
        edgeButton.setAttribute("class", "hidden");
        edgeButton.setAttribute("title", "Draw edge labels");
        document.drawEdgeLabels = false;
        d3.selectAll(".edgeLabel").style("visibility", "hidden");
      }
    }
    if (document.drawEdgeLabels === false) {
      edgeButton.setAttribute("class", "hidden");
      edgeButton.setAttribute("title", "Draw edge labels");
      d3.selectAll(".edgeLabel").style("visibility", "hidden");
    } else {
      edgeButton.setAttribute("class", "visible");
      edgeButton.setAttribute("title", "Hide edge labels");
      d3.selectAll(".edgeLabel").style("visibility", "visible");
      document.drawEdgeLabels = true;
    }
    this._header.appendChild(edgeButton);
    this._triggerEdgeLabelsButton = edgeButton;

}

Graph.prototype.clearHeader = function() {
  removeElementIfExists(this._triggerNodeLabelsButton);
  removeElementIfExists(this._triggerEdgeLabelsButton);
}
