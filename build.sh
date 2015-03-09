assemblyJar=$1
jarjarjar=$assemblyJar-jarjared.jar

echo "Jarjaring $assemblyJar..."
java -jar build/jarjar-1.4.jar process build/rules.txt $assemblyJar $jarjarjar

oldSprayConf=reference.conf
echo "Deleting $oldSprayConf from $jarjarjar..."
zip -d $jarjarjar $oldSprayConf >> build.log

newSprayConf=dds.conf
echo "Including $newSprayConf into $jarjarjar..."
cd build
zip -g $jarjarjar $newSprayConf >> build.log
cd ..

echo "Replacing $assemblyJar by $jarjarjar"
mv $jarjarjar $assemblyJar
