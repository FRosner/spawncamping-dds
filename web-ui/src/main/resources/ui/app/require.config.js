var libFolder = "../lib/";
var appFolder = "../app/";
var slickGridFolder = "slickgrid/";

require.config({
  paths: {
    jquery: libFolder + "jquery-1.7.min",
    dragevent: libFolder + "jquery.event.drag-2.2.min",

    d3: libFolder + "d3.v3.min",
    d3tip: libFolder + "d3.tip.v0.6.3.min",
    c3: libFolder + "c3.min",

    chroma: libFolder + "chroma.min",

    underscore: libFolder + "underscore.min",

    slickcore: libFolder + slickGridFolder + "slick.core.min",
    slickgrid: libFolder + slickGridFolder + "slick.grid.min",
    slickdataview: libFolder + slickGridFolder + "slick.dataview.min",
    slickpager: libFolder + slickGridFolder + "slick.pager.min",

    dds: libFolder + "dds.min"

  }
});

require(["d3"], function(d3) {
  console.debug("Loading d3 v" + d3.version);
});

require.config({
  shim: {
    dragevent:     ["jquery"],

    slickcore:     ["dragevent"],
    slickgrid:     ["slickcore", "dragevent"],
    slickdataview: ["slickgrid"],
    slickpager:    ["slickgrid"],

    d3tip: ["d3"],

    dds: {
        deps: ["d3", "d3tip", "c3", "chroma", "slickcore", "slickgrid",
               "slickdataview", "slickpager"],
        exports: "dds"
    }
  }
});
