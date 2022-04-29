cd ..

mkdir statistics_classes
javac -cp $(hadoop classpath) -d statistics_classes statistics/src/main/java/*.java
jar -cvf statistics.jar -C statistics_classes/ .

rm -r statistics_classes