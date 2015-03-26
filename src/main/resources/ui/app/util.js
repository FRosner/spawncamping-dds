function doAndRedoOnResize(f) {
    f();
    window.onresize = f;
}
