jarName=RelatedRecModel.jar
movieItemFeatPath=$movieItemFets   # 调用俄ext1.sh export 的变>量
jarPath=/home/tvapk/run_sh/cuiliqing/
hdfs=/user/tvapk/cuiliqing/
hadoop=/opt/hadoop/hadoop-2.6.0-cdh5.7.1/bin/hadoop
dataSavePath=/data/tvapk/cuiliqing/cuiliqing/
logdata=/logdata/tvapk/android_event/
#tagsIdStart=$mvTagsStartId
joinStart=$joinStartId

xgbClkIn=/user/tvapk/cuiliqing/xgbClkPosIn

$hadoop fs -rm -r $xgbClkIn
$hadoop fs -mkdir $xgbClkIn
#$hadoop fs -rm -r $xgbDisIn
#$hadoop fs -mkdir $xgbDisIn

xgbreOut=/user/tvapk/cuiliqing/xgbRelatedPosOut

for i in `seq 40 69`; do
     echo "---------------- $i ---------------- "
     dateStart=`date -d "$i days ago " "+%Y%m%d"`
      #Arr[$i-5]=$dateStart
     echo "----------- $dateStart -------------" 
     dateEnd=$dateStart
     $hadoop fs -rm -r $xgbreOut
     $hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemClks $logdata  $xgbreOut $dateStart $dateEnd
     $hadoop fs -mv $xgbreOut/part-r-00000 $xgbClkIn/$dateStart"clk"
     #$hadoop fs -rm -r $xgbreOut
     #$hadoop jar $jarPath$jarName com.qiguo.tv.movie.relatedRecXgb.ExtRelatedItemAllShws $logdata $xgbreOut $dateStart $dateEnd
     #$hadoop fs -mv $xgbreOut/part-r-00000 $xgbDisIn/$dateStart"shw"
     #$hadoop fs -rm -r $xgbreOut
done
