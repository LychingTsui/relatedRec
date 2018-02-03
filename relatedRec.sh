#! /usr/bin/sh
##在inputDir路径下需要的文件有 movieItemFeatures relatedModel 
group=":D:104"
outfile=/user/tvapk/cuiliqing/reltOut
hdfs=/user/tvapk/cuiliqing
hadoop=/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop
dataSavePath=/data/tvapk/cuiliqing/cuiliqing

inputDir=$dataSavePath/relatedRecByModel3       ####固定输入路径
jarPth=/home/tvapk/run_sh/cuiliqing
jar=RelatedRecModel.jar
model=relatedModel
mvItemFeats=movieItemFeatures

flag=`sed -n '3p' $inputDir/relatedModel | awk '{print $2}'`
mvTagsIdStart=`sed -n '2p' $inputDir/relatedFeatsIdx.txt | awk '{print $2}'`  ## 此处取特征id索引文件的2th
#行，具体看2th对应起始特征id，有可能是tags，也有可能是area
joinTagsIdStart=`sed -n '3p' $inputDir/relatedFeatsIdx.txt | awk '{print $2}'` # 在取join交叉特征起始id时
#又可能是3th，or 4th
modeltotRow=`cat $inputDir/relatedModel | wc -l`
sed -n "7,${modeltotRow}p" $inputDir/relatedModel  | awk '{print NR,$1}' > $inputDir/wei.txt

$hadoop fs -rm -r $hdfs/relatedRecCache
$hadoop fs -mkdir $hdfs/relatedRecCache

$hadoop fs -put $inputDir/wei.txt $hdfs/relatedRecCache/
$hadoop fs -put $inputDir/$mvItemFeats $hdfs/relatedRecCache/

$hadoop fs -rm -r $hdfs/$mvItemFeats 
$hadoop fs -put $inputDir/$mvItemFeats $hdfs/ 

$hadoop fs -rm -r $outfile
$hadoop jar $jarPth/$jar com.qiguo.tv.movie.relatedRec.TopkPredByRelatedRecModelFullJoin  $hdfs/$mvItemFeats $outfile $hdfs/relatedRecCache $flag $mvTagsIdStart $joinTagsIdStart $group
$hadoop fs -rm -r $hdfs/relatedMvRec
$hadoop fs -cp $outfile/part-r-00000 $hdfs/relatedMvRec
$hadoop fs -rm -r $outfile 
rm  $dataSavePath/relatedMvRec
$hadoop fs -get $hdfs/relatedMvRec $dataSavePath/
$hadoop fs -rm -r $hdfs/relatedMvRec 
$hadoop fs -rm -r $hdfs/$mvItemFeats 
