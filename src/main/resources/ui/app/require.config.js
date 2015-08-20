var libFolder = "../lib/";
var appFolder = "../app/";
var slickGridFolder = "slickgrid/";

require.config({
  shim: {
    dragevent:     ['jquery'],
    "d3.parcoords": {
      deps: [
        "d3-global"
      ],
      exports: "d3.parcoords"
    },

    slickcore:     ['dragevent'],
    slickgrid:     ['slickcore', 'dragevent'],
    slickdataview: ['slickgrid'],
    slickpager:    ['slickgrid']
  },
  paths: {
    jquery: libFolder + "jquery-1.7.min",
    dragevent: libFolder + "jquery.event.drag-2.2.min",

    d3: libFolder + "d3.v3.min",
    parcoords: libFolder + "d3.parcoords.min",
    c3: libFolder + "c3.min",

    chroma: libFolder + "chroma.min",

    underscore: libFolder + "underscore.min",

    slickcore: libFolder + slickGridFolder + "slick.core.min",
    slickgrid: libFolder + slickGridFolder + "slick.grid.min",
    slickdataview: libFolder + slickGridFolder + "slick.dataview.min",
    slickpager: libFolder + slickGridFolder + "slick.pager.min"
  }
});

define("d3-global", ["d3"], function(_) {
  window.d3 = _;
});
