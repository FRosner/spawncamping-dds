function doAndRedoOnResize(f) {
    f();
    window.onresize = f;
}

function flatMap(seq, f) {
    return _.flatten(seq.map(f), true);
}

function removeElementIfExists(element) {
    if (element != null) {
        element.parentNode.removeChild(element);
    }
}

function removeElementByIdIfExists(elementId) {
    removeElementIfExists(document.getElementById(elementId));
}
