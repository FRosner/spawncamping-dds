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

});
