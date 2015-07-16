describe('util.js', function() {

  it('flatMaps one level correctly', function() {
      expect(flatMap([1,2,3], function(element) {
        return [element - 1, element, element + 1]
      })).toEqual([0,1,2,1,2,3,2,3,4]);
  });

  it('flatMaps two levels correctly', function() {
    expect(flatMap([1,2,3], function(element) {
      return [[element - 1, element, element + 1]]
    })).toEqual([[0,1,2],[1,2,3],[2,3,4]]);
  });

  it('does and redoes on resize correctly', function() {
    var x = 0
    var f = function() {
      x = x + 1;
    };
    var w = {};
    doAndRedoOnResizeOf(w, f)
    expect(x).toEqual(1);
    w.onresize();
    expect(x).toEqual(2);
  });

});
