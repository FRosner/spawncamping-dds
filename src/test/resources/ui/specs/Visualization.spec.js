define(['Visualization'], function(Visualization) {

  describe('Visualization', function() {

    it('has a working title setter and getter', function() {
        var vis = new Visualization();
        vis.title("test");
        expect(vis.title()).toEqual("test");
    });

    it('has a working header setter and getter', function() {
        // TODO: refactoring; not testable because of dependency to global document variable
    });

    it('has a working content setter and getter', function() {
        // TODO: refactoring; not testable because of dependency to global document variable
    });

    it('has a working margin setter and getter', function() {
        var vis = new Visualization();
        vis.margin({
          top: 1,
          bottom: 3,
          left: 3,
          right: 7
        });
        expect(vis.margin()).toEqual({
          top: 1,
          bottom: 3,
          left: 3,
          right: 7
        });
    });

    it('its margin setter fills missing margin values with default', function() {
        var vis = new Visualization();
        vis.margin({
          bottom: 3,
          left: 3,
          right: 7
        });
        expect(vis.margin()).toEqual({
          top: 0,
          bottom: 3,
          left: 3,
          right: 7
        });
    });

    it('has a working width setter and getter', function() {
        var vis = new Visualization();
        vis.width(5);
        expect(vis.width()).toEqual(5);
    });

    it('has a working height setter and getter', function() {
        var vis = new Visualization();
        vis.height(10);
        expect(vis.height()).toEqual(10);
    });

    it('has a working data setter and getter', function() {
        var vis = new Visualization();
        vis.data({val : "test"});
        expect(vis.data()).toEqual({val : "test"});
    });

    it('has a working draw method', function() {
        // TODO: refactoring; not testable because of not working content setter
    });

    it('has a working clear method', function() {
        // TODO: refactoring; not testable because of not working header setter
    });

  });

});
