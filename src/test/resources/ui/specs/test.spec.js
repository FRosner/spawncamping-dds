describe('Hello world', function() {

  it('says hello', function() {
      expect("Hello World!").toEqual("Hello World!");
  });

  it('fails', function() {
      expect("Hello world!").toEqual("Hello World!");
  });
  
});
