function doAndRedoOnResize(f) {
    f();
    window.onresize = f;
}

function flatMap(seq, f) {
    return _.flatten(seq.map(f), true);
}
