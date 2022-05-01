mkdir kmeans_classes
javac -cp "$(hadoop classpath)" -d kmeans_classes src/*.java
jar -cvf kmeans.jar -C kmeans_classes/ .




