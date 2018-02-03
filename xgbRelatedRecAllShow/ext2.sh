#! /usr/bin/sh

#dateStart=20171217
#dateEnd=20180116
jarName=RelatedRecModel.jar
movieItemFeatPath=$movieItemFets   # 调用俄ext1.sh export 的变量
jarPath=/home/tvapk/run_sh/cuiliqing/
hdfs=/user/tvapk/cuiliqing/
hadoop=/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop
dataSavePath=/data/tvapk/cuiliqing/cuiliqing/
logdata=/logdata/tvapk/android_event/
#tagsIdStart=$mvTagsStartId
joinStart=$joinStartId
xgbin=/user/tvapk/cuiliqing/xgbInput

$hadoop fs -rm -r $xgbin
$hadoop fs -mkdir $xgbin 

xgbreOut=/user/tvapk/cuiliqing/xgbRelatedOut

for i in `seq 3 32`; do
     echo "---------------- $i ---------------- "
    dateStart=`date -d "$i days ago " "+%Y%m%d"`
    #Arr[$i-5]=$dateStart
    dateEnd=$dateStart
    $hadoop fs -rm -r $xgbreOut
    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemClks $logdata $xgbreOut $dateStart $dateEnd
    $hadoop fs -cp $xgbreOut/part-r-00000 $xgbin/$dateStart"clk"
    $hadoop fs -rm -r $xgbreOut 

    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemAllShws $logdata $xgbreOut $dateStart $dateEnd
    $hadoop fs -cp $xgbreOut/part-r-00000 $xgbin/$dateStart"shw" 
    $hadoop fs -rm -r $xgbreOut 
done
$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemPairRateAllShws $xgbin/ $xgbreOut 
$hadoop fs -rm -r $hdfs"xgbMvpair" 
$hadoop fs -cp $xgbreOut/part-r-00000 $hdfs"xgbMvpair"
$hadoop fs -rm -r $xgbreOut

$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItem2Features $hdfs"xgbMvpair" $xgbreOut $hdfs$movieItemFeatPath $joinStart

$hadoop fs -rm -r $hdfs"xgbTraindataSet"
$hadoop fs -cp $xgbreOut/part-r-00000 $hdfs"xgbTraindataSet" 
$hadoop fs -rm -r $xgbreOut 

