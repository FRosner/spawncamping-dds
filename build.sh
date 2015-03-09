assemblyJar=$1
jarjarjar=$assemblyJar-jarjared.jar

echo "Jarjaring $assemblyJar..."
java -jar build/jarjar-1.4.jar process build/rules.txt $assemblyJar $jarjarjar

sprayConf=reference.conf
echo "Deleting $sprayConf from $jarjarjar..."
zip -d $jarjarjar $sprayConf > /dev/null

echo "Replacing $assemblyJar by $jarjarjar"
mv $jarjarjar $assemblyJar
