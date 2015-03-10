assemblyJar=$1
jarjarjar=$assemblyJar-jarjared.jar

echo "[info] Jarjaring $assemblyJar into $jarjarjar"
java -jar build/jarjar-1.4.jar process build/rules.txt $assemblyJar $jarjarjar

oldSprayConf=reference.conf
echo "[info] Deleting $oldSprayConf from $jarjarjar"
zip -d $jarjarjar $oldSprayConf >> build.log

newSprayConf=dds.conf
echo "[info] Including $newSprayConf into $jarjarjar"
cd build
zip -g $jarjarjar $newSprayConf >> build.log
cd ..

echo "[info] Replacing $assemblyJar by $jarjarjar"
mv $jarjarjar $assemblyJar
