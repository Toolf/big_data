javac -cp /usr/local/hadoop/share/hadoop/common/hadoop-common-3.3.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.4.jar:/usr/local/hadoop/share/hadoop/common/lib/commons-cli-1.2.jar -d bin/ src/WordCount.java

cd bin
jar cvf HadoopMarketAverage.jar bin