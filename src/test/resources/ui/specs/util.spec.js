define(["util"], function(Util) {

  describe('util', function() {

    it('flatMaps one level correctly', function() {
        expect(Util.flatMap([1,2,3], function(element) {
          return [element - 1, element, element + 1]
        })).toEqual([0,1,2,1,2,3,2,3,4]);
    });

    it('flatMaps two levels correctly', function() {
      expect(Util.flatMap([1,2,3], function(element) {
        return [[element - 1, element, element + 1]]
      })).toEqual([[0,1,2],[1,2,3],[2,3,4]]);
    });

    it('does and redoes on resize correctly', function() {
      var x = 0
      var f = function() {
        x = x + 1;
      };
      var w = {};
      Util.doAndRedoOnResizeOf(w, f)
      expect(x).toEqual(1);
      w.onresize();
      expect(x).toEqual(2);
    });

    it('removes an existing element', function() {
      var removedChild = {};
      var parent = {
        id : 0,
        removeChild : function(child) {
          removedChild = child;
        }
      };
      var child = {
        id : 1,
        parentNode: parent
      };
      Util.removeElementIfExists(child);
      expect(removedChild).toEqual(child);
    });

  });

});
