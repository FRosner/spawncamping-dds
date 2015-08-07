var mainRequireConf = EnvJasmine.rootDir + "require.config.js";
console.log("[info] Loading " + mainRequireConf);
EnvJasmine.loadGlobal(mainRequireConf);

var testRequireConf = EnvJasmine.testDir + "require.config.js";
console.log("[info] Loading " + testRequireConf);
EnvJasmine.loadGlobal(testRequireConf);
