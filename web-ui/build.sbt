//////////////////
// Test Options //
//////////////////
Seq(jasmineSettings: _*)

(test in Test) <<= (test in Test) dependsOn (jasmine)

appJsDir <+= sourceDirectory { src => src / "main" / "resources" / "ui" / "app" }

appJsLibDir <+= sourceDirectory {  src => src / "main" / "resources" / "ui" / "lib" }

jasmineTestDir <+= sourceDirectory { src => src / "test" / "resources" / "ui" }

jasmineConfFile <+= sourceDirectory { src => src / "test" / "resources" / "ui" / "test.dependencies.js" }

jasmineRequireJsFile <+= sourceDirectory { src => src / "main" / "resources" / "ui" / "lib" / "require.js" }
