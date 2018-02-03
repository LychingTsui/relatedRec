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

xgbClkIn=/user/tvapk/cuiliqing/xgbClkIn
xgbDisIn=/user/tvapk/cuiliqing/xgbDisIn

$hadoop fs -rm -r $xgbClkIn
$hadoop fs -mkdir $xgbClkIn
$hadoop fs -rm -r $xgbDisIn
$hadoop fs -mkdir $xgbDisIn

xgbreOut=/user/tvapk/cuiliqing/xgbRelatedOut

for i in `seq 3 32`; do
     echo "---------------- $i ---------------- "
    dateStart=`date -d "$i days ago " "+%Y%m%d"`
    #Arr[$i-5]=$dateStart
    dateEnd=$dateStart
    $hadoop fs -rm -r $xgbreOut
    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemClks $logdata $xgbreOut $dateStart $dateEnd
    $hadoop fs -mv $xgbreOut/part-r-00000 $xgbClkIn/$dateStart"clk"
    $hadoop fs -rm -r $xgbreOut 

    $hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemAllShws $logdata $xgbreOut $dateStart $dateEnd
    $hadoop fs -mv $xgbreOut/part-r-00000 $xgbDisIn/$dateStart"shw" 
    $hadoop fs -rm -r $xgbreOut 
done

collectClk=/user/tvapk/cuiliqing/collectClk
collectDis=/user/tvapk/cuiliqing/collectDis
$hadoop fs -rm -r $collectClk
$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.CollectRelatedItems $xgbClkIn/  $collectClk
$hadoop fs -rm -r $collectDis 
$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.CollectRelatedItems $xgbDisIn/  $collectDis

xgbInput=/user/tvapk/cuiliqing/xgbInput
$hadoop fs -rm -r $xgbInput 
$hadoop fs -mkdir $xgbInput
$hadoop fs -mv $collectClk/part-r-00000 $xgbInput/part-r-00000
$hadoop fs -mv $collectDis/part-r-00000 $xgbInput/part-r-00001
$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemPairRateAllShws $xgbInput/ $xgbreOut 

$hadoop fs -rm -r $hdfs"xgbMvpair" 
$hadoop fs -cp $xgbreOut/part-r-00000 $hdfs"xgbMvpair"
$hadoop fs -rm -r $xgbreOut

$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItem2Features $hdfs"xgbMvpair" $xgbreOut $hdfs$movieItemFeatPath $joinStart

$hadoop fs -rm -r $hdfs"xgbTraindataSet"
$hadoop fs -cp $xgbreOut/part-r-00000 $hdfs"xgbTraindataSet" 
$hadoop fs -rm -r $xgbreOut 

